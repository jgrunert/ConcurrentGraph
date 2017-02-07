package mthesis.concurrent_graph.worker;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.BaseQueryGlobalValues.QueryStats;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.GetToKnowMessage;
import mthesis.concurrent_graph.communication.InvalidateRegisteredVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.MoveVerticesMessage;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.communication.VertexMessageBucket;
import mthesis.concurrent_graph.util.MiscUtil;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

/**
 * Concurrent graph processing worker main
 *
 * @author Jonas Grunert
 *
 * @param <V>
 *            Vertex type
 * @param <E>
 *            Edge type
 * @param <M>
 *            Vertex message type
 * @param <Q>
 *            Global query values type
 */
public class WorkerMachine<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues>
extends AbstractMachine<V, E, M, Q> implements VertexWorkerInterface<V, E, M, Q> {

	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String outputDir;
	private volatile boolean started = false;
	private List<String> assignedPartitions;

	private final JobConfiguration<V, E, M, Q> jobConfig;
	private final BaseVertexInputReader<V, E, M, Q> vertexReader;

	//private List<AbstractVertex<V, E, M, Q>> localVerticesList;
	private final Map<Integer, AbstractVertex<V, E, M, Q>> localVertices = new HashMap<>();

	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Global, aggregated QueryGlobalValues. Aggregated and sent by master
	private final Map<Integer, WorkerQuery<V, E, M, Q>> activeQueries = new HashMap<>();
	private final List<WorkerQuery<V, E, M, Q>> activeQueriesThisSuperstep = new ArrayList<>();

	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	// Buckets to collect messages to send
	private final VertexMessageBucket<M> vertexMessageBroadcastBucket = new VertexMessageBucket<>();
	private final Map<Integer, VertexMessageBucket<M>> vertexMessageMachineBuckets = new HashMap<>();

	private final ConcurrentLinkedQueue<ControlMessage> receivedControlMessages = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<VertexMessage<V, E, M, Q>> receivedVertexMessages = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<GetToKnowMessage> receivedGetToKnowMessages = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<MoveVerticesMessage<V, E, M, Q>> receivedMoveVerticesMessages = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<InvalidateRegisteredVerticesMessage> receivedInvVertsMessages = new ConcurrentLinkedQueue<>();



	public WorkerMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, int masterId, String outputDir,
			JobConfiguration<V, E, M, Q> jobConfig,
			BaseVertexInputReader<V, E, M, Q> vertexReader) {
		super(machines, ownId, jobConfig);
		this.masterId = masterId;
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		for (final Integer workerId : otherWorkerIds) {
			vertexMessageMachineBuckets.put(workerId, new VertexMessageBucket<>());
		}
		vertexValueFactory = jobConfig.getVertexValueFactory();
		globalValueFactory = jobConfig.getGlobalValuesFactory();

		this.outputDir = outputDir;
		this.jobConfig = jobConfig;
		this.vertexReader = vertexReader;
	}

	private void loadVertices(List<String> partitions) {
		List<AbstractVertex<V, E, M, Q>> localVerticesList = vertexReader.getVertices(partitions, jobConfig, this);
		for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
			localVertices.put(vertex.ID, vertex);
		}
	}



	@Override
	public void run() {
		logger.info("Starting run worker node " + ownId);

		try {
			while (!started) {
				Thread.sleep(100);
				handleReceivedMessages();
			}

			// Initialize, load assigned partitions
			loadVertices(assignedPartitions);
			sendMasterInitialized();

			// Execution loop
			while (!Thread.interrupted()) {
				// Wait for queries
				while (activeQueries.isEmpty()) {
					Thread.sleep(1);
					handleReceivedMessages();
				}

				// Wait for ready queries
				activeQueriesThisSuperstep.clear();
				while (true) {
					handleReceivedMessages();

					for (WorkerQuery<V, E, M, Q> activeQuery : activeQueries.values()) {
						if ((activeQuery.getStartedSuperstepNo() > activeQuery.getCalculatedSuperstepNo()))
							activeQueriesThisSuperstep.add(activeQuery);
					}
					if (!activeQueriesThisSuperstep.isEmpty())
						break;
					Thread.sleep(1);
				}


				// Compute active queries
				for (WorkerQuery<V, E, M, Q> activeQuery : activeQueriesThisSuperstep) {
					{
						int queryId = activeQuery.Query.QueryId;
						int superstepNo = activeQuery.getStartedSuperstepNo();

						// Next superstep. Compute and Messaging (done by vertices)
						logger.debug("Worker start query " + queryId + " superstep compute " + superstepNo);

						if (!activeQuery.MovedVertices.isEmpty()) {
							for (AbstractVertex<V, E, M, Q> v : activeQuery.MovedVertices) {
								localVertices.put(v.ID, v);
							}
							activeQuery.MovedVertices.clear();
						}

						// First frame: Call all vertices, second frame only active vertices
						long startTime = System.currentTimeMillis();
						if (superstepNo == 0) {
							for (final AbstractVertex<V, E, M, Q> vertex : localVertices.values()) {
								vertex.superstep(superstepNo, activeQuery);
							}
						}
						else {
							for (AbstractVertex<V, E, M, Q> vertex : activeQuery.ActiveVerticesThis.values()) {
								vertex.superstep(superstepNo, activeQuery);
							}
						}
						activeQuery.calculatedSuperstep();
						activeQuery.QueryLocal.Stats.ComputeTime = System.currentTimeMillis() - startTime;


						// Barrier sync with other workers;
						if (!otherWorkerIds.isEmpty()) {
							flushVertexMessages(activeQuery);
							sendWorkersSuperstepFinished(activeQuery);
						}
						else {
							checkSuperstepBarrierFinished(activeQuery);
						}
						logger.debug("Worker finished compute " + queryId + ":" + superstepNo);
					}

					// Finish barrier sync if received all barrier syncs from other workers
					if (!otherWorkerIds.isEmpty()) {
						checkSuperstepBarrierFinished(activeQuery);
					}

					// Handle received messages after each query
					handleReceivedMessages();
				}
			}
		}
		catch (final InterruptedException e) {
			return;
		}
		catch (final Exception e) {
			logger.error("Exception at worker run", e);
		}
		finally {
			logger.info("Worker closing");
			messaging.getReadyForClose();
			try {
				Thread.sleep(200); // TODO Cleaner solution, for example a final message from master
			}
			catch (final InterruptedException e) {
				e.printStackTrace();
			}
			stop();
		}
	}

	/**
	 * Handles all messages in received queues.
	 */
	private void handleReceivedMessages() {
		while (!receivedControlMessages.isEmpty())
			handleControlMessage(receivedControlMessages.poll());
		while (!receivedVertexMessages.isEmpty())
			handleVertexMessage(receivedVertexMessages.poll());
		while (!receivedGetToKnowMessages.isEmpty())
			handleGetToKnowMessage(receivedGetToKnowMessages.poll());
		while (!receivedMoveVerticesMessages.isEmpty())
			handleMoveVerticesMessage(receivedMoveVerticesMessages.poll());
		while (!receivedInvVertsMessages.isEmpty())
			handleInvalidateRegisteredVerticesMessage(receivedInvVertsMessages.poll());
	}


	public void flushVertexMessages(WorkerQuery<V, E, M, Q> query) {
		sendBroadcastVertexMessageBucket(query, query.getStartedSuperstepNo());
		for (final int otherWorkerId : otherWorkerIds) {
			final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(otherWorkerId);
			if (!msgBucket.messages.isEmpty())
				sendUnicastVertexMessageBucket(msgBucket, otherWorkerId, query, query.getStartedSuperstepNo());
			messaging.flushChannel(otherWorkerId);
		}
	}


	private void sendWorkersSuperstepFinished(WorkerQuery<V, E, M, Q> workerQuery) {
		messaging.sendControlMessageMulticast(otherWorkerIds,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepBarrier(workerQuery.getStartedSuperstepNo(), ownId, workerQuery.Query),
				true);
	}

	private void sendMasterInitialized() {
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Initialized(ownId, localVertices.size()),
				true);
	}

	private void sendMasterSuperstepFinished(WorkerQuery<V, E, M, Q> workerQuery, Map<Integer, Integer> queryIntersects) {
		// Clear vertex move waits before
		workerQuery.VertexMovesWaitingFor.clear();
		workerQuery.VertexMovesReceived.clear();

		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(workerQuery.getStartedSuperstepNo(), ownId,
						workerQuery.QueryLocal, queryIntersects),
				true);
	}

	private void sendMasterQueryFinishedMessage(WorkerQuery<V, E, M, Q> workerQuery) {
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QueryFinished(workerQuery.getStartedSuperstepNo(), ownId, workerQuery.QueryLocal),
				true);
	}

	/**
	 * Sends a vertex message. If local vertex, direct loopback. It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	@Override
	public void sendVertexMessage(int dstVertex, M messageContent, WorkerQuery<V, E, M, Q> query) {
		final AbstractVertex<V, E, M, Q> msgVert = localVertices.get(dstVertex);
		if (msgVert != null) {
			// Local message
			query.QueryLocal.Stats.MessagesTransmittedLocal++;
			List<M> queryInMsgs = msgVert.queryMessagesNextSuperstep.get(query.QueryId);
			if (queryInMsgs == null) {
				// Add queue if not already added
				queryInMsgs = new ArrayList<>();
				msgVert.queryMessagesNextSuperstep.put(query.QueryId, queryInMsgs);
			}
			queryInMsgs.add(messageContent);
			// Activate vertex
			query.ActiveVerticesNext.put(msgVert.ID, msgVert);
		}
		else {
			// Remote message
			final Integer remoteMachine = remoteVertexMachineRegistry.lookupEntry(dstVertex);
			if (remoteMachine != null) {
				// Unicast remote message
				sendVertexMessageToMachine(remoteMachine, dstVertex, query, messageContent);
			}
			else {
				// Broadcast remote message
				query.QueryLocal.Stats.MessagesSentBroadcast += otherWorkerIds.size();
				vertexMessageBroadcastBucket.addMessage(dstVertex, messageContent);
				if (vertexMessageBroadcastBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
					sendBroadcastVertexMessageBucket(query, query.getStartedSuperstepNo());
				}
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int dstMachine, int dstVertex, WorkerQuery<V, E, M, Q> query, M messageContent) {
		final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(dstMachine);
		msgBucket.addMessage(dstVertex, messageContent);
		if (msgBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
			sendUnicastVertexMessageBucket(msgBucket, dstMachine, query, query.getStartedSuperstepNo());
		}
		query.QueryLocal.Stats.MessagesSentUnicast++;
	}

	private void sendUnicastVertexMessageBucket(VertexMessageBucket<M> msgBucket, int dstMachine, WorkerQuery<V, E, M, Q> query,
			int superstepNo) {
		final List<Pair<Integer, M>> msgList = packVertexMessage(msgBucket);
		messaging.sendVertexMessageUnicast(dstMachine, superstepNo, ownId, query.QueryId, msgList);
		query.QueryLocal.Stats.MessageBucketsSentUnicast++;
	}

	private void sendBroadcastVertexMessageBucket(WorkerQuery<V, E, M, Q> query, int superstepNo) {
		final List<Pair<Integer, M>> msgList = packVertexMessage(vertexMessageBroadcastBucket);
		messaging.sendVertexMessageBroadcast(otherWorkerIds, superstepNo, ownId, query.QueryId, msgList);
		query.QueryLocal.Stats.MessageBucketsSentBroadcast++;
	}

	private List<Pair<Integer, M>> packVertexMessage(VertexMessageBucket<M> msgBucket) {
		final List<Pair<Integer, M>> msgList = new ArrayList<>(msgBucket.messages); // TODO Pool instance
		msgBucket.messages.clear();
		return msgList;
	}

	// TODO Flush all channels at end of frame



	@Override
	public void onIncomingControlMessage(ControlMessage message) {
		receivedControlMessages.add(message);
	}

	public void handleControlMessage(ControlMessage message) {
		try {
			if (message != null) {
				switch (message.getType()) {
					case Master_Worker_Initialize:
						assignedPartitions = message.getAssignPartitions().getPartitionFilesList();
						started = true;
						break;

					case Master_Query_Start:
						startQuery(deserializeQuery(message.getQueryValues()));
						break;

					case Master_Query_Next_Superstep: {
						prepareNextSuperstep(message);
					}
					break;

					case Master_Query_Finished: {
						Q query = deserializeQuery(message.getQueryValues());
						finishQuery(activeQueries.get(query.QueryId));
					}
					break;


					case Worker_Query_Superstep_Barrier: {
						Q query = deserializeQuery(message.getQueryValues());
						WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(query.QueryId);

						// We have to wait if the query is not already started
						while (activeQuery == null) {
							logger.warn("Waiting for not yet started query " + query.QueryId);
							Thread.sleep(1);
							activeQuery = activeQueries.get(query.QueryId);
						}

						if (message.getSuperstepNo() == activeQuery.getStartedSuperstepNo()) {
							// Remove worker from ChannelBarrierWaitSet, wait finished, correct superstep
							activeQuery.ChannelBarrierWaitSet.remove(message.getSrcMachine());
							checkSuperstepBarrierFinished(activeQuery);
						}
						else if (message.getSuperstepNo() == activeQuery.getStartedSuperstepNo() + 1) {
							// We received a superstep barrier sync before even starting it. Remember it for later
							activeQuery.ChannelBarrierPremature.add(message.getSrcMachine());
							//							logger.warn("Premature " + query.QueryId + ":" + message.getSuperstepNo()
							//									+ " at " + ownId + " from " + message.getSrcMachine());
						}
						else {
							// Completely wrong superstep
							logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: " + message.getSuperstepNo()
							+ " at " + activeQuery.Query.QueryId + ":" + activeQuery.getStartedSuperstepNo());
						}
					}
					break;

					case Master_Shutdown: {
						logger.info("Received shutdown signal");
						stop();
					}
					break;

					default:
						logger.error("Unknown control message type: " + message);
						break;
				}
			}
		}
		catch (InterruptedException e) {
			return;
		}
		catch (Exception e) {
			logger.error("exception at incomingControlMessage", e);
		}
	}

	/**
	 * Called after Master_Query_Next_Superstep message.
	 * Prepare next superstep and start if already possible.
	 */
	private void prepareNextSuperstep(ControlMessage message) {
		Q msgQuery = deserializeQuery(message.getQueryValues());
		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(msgQuery.QueryId);

		// Checks
		if (activeQuery == null) {
			logger.error("Received Master_Next_Superstep for unknown query: " + message);
			return;
		}
		if (message.getSuperstepNo() != activeQuery.getCalculatedSuperstepNo() + 1) {
			logger.error("Received Master_Next_Superstep with wrong superstepNo: " + message.getSuperstepNo() + " at step "
					+ activeQuery.getCalculatedSuperstepNo());
			return;
		}

		// Start new superstep query values
		activeQuery.Query = msgQuery;
		activeQuery.QueryLocal = globalValueFactory.createClone(msgQuery);
		activeQuery.QueryLocal.Stats = new QueryStats();

		{
			// Handle vertex moving if master tells us to do so
			if (message.hasSendQueryVertices()) {
				// Send query vertices to other worker
				int sendTo = message.getSendQueryVertices().getSendToMachine();
				sendQueryVertices(activeQuery, sendTo);
			}
			else if (message.hasReceiveQueryVertices()) {
				List<Integer> recvFrom = message.getReceiveQueryVertices().getRecvFromMachineList();
				if (!recvFrom.isEmpty()) {
					// Wait for receiving vertices from other worker
					assert activeQuery.VertexMovesWaitingFor.isEmpty();
					activeQuery.VertexMovesWaitingFor.addAll(recvFrom);
				}
			}

			// Start next superstep if waiting for no vertices to move
			activeQuery.preparedSuperstep();

			if (activeQuery.VertexMovesWaitingFor.isEmpty()
					|| activeQuery.VertexMovesReceived.size() == activeQuery.VertexMovesWaitingFor.size()) {
				assert activeQuery.VertexMovesWaitingFor.isEmpty()
				|| activeQuery.VertexMovesReceived.containsAll(activeQuery.VertexMovesWaitingFor);
				startNextSuperstep(activeQuery);
			}
		}
	}

	/**
	 * Called as soon as a query is ready for compute.
	 * After Master_Query_Next_Superstep and received/sent vertices.
	 */
	private void startNextSuperstep(WorkerQuery<V, E, M, Q> activeQuery) {
		// Mark query as ready for compute superstep
		activeQuery.startNextSuperstep();

		// Start new barrier sync
		if (!activeQuery.ChannelBarrierPremature.isEmpty()) {
			activeQuery.ChannelBarrierWaitSet.removeAll(activeQuery.ChannelBarrierPremature);
		}
		activeQuery.ChannelBarrierPremature.clear();
		// Check if we already received all barrier syncs from other workers, directly finish superstep
		checkSuperstepBarrierFinished(activeQuery);
	}

	/**
	 * Sends all active vertices of a query if they are only active at this query.
	 * Used for incremental vertex migration.
	 */
	private void sendQueryVertices(WorkerQuery<V, E, M, Q> query, int sendToWorker) {
		int queryId = query.QueryId;
		List<AbstractVertex<V, E, M, Q>> verticesToMove = new ArrayList<>();
		for (AbstractVertex<V, E, M, Q> activeVertex : query.ActiveVerticesThis.values()) {
			localVertices.remove(activeVertex.ID);
			verticesToMove.add(activeVertex);
			// Send vertices if bucket full
			if (verticesToMove.size() >= Settings.VERTEX_MOVE_BUCKET_MAX_VERTICES) {
				messaging.sendMoveVerticesMessage(sendToWorker, verticesToMove, queryId, false);
				verticesToMove = new ArrayList<>();
			}
		}
		query.ActiveVerticesThis.clear();
		// Send remaining vertices
		messaging.sendMoveVerticesMessage(sendToWorker, verticesToMove, queryId, true);
	}


	/**
	 * Called to check if superstep barrier syncs received and finishes superstep barrier if possible.
	 * Will not finish the superstep barrier if already finished or if local superstep calculation not finished.
	 * @param activeQuery
	 */
	private void checkSuperstepBarrierFinished(WorkerQuery<V, E, M, Q> activeQuery) {
		{
			if (!activeQuery.ChannelBarrierWaitSet.isEmpty()) {
				// Not all barrier syncs received yet
				return;
			}

			if (!(activeQuery.getCalculatedSuperstepNo() == activeQuery.getStartedSuperstepNo() &&
					activeQuery.getBarrierFinishedSuperstepNo() + 1 == activeQuery.getCalculatedSuperstepNo())) {
				// Already finished or local superstep calculation not finished.
				return;
			}

			if (!(activeQuery.VertexMovesWaitingFor.isEmpty()
					|| activeQuery.VertexMovesWaitingFor.size() == activeQuery.VertexMovesReceived.size())) {
				throw new RuntimeException("Nooooooooooooooooooo");
				//				assert activeQuery.VertexMovesWaitingFor.isEmpty() || activeQuery.VertexMovesWaitingFor.equals(activeQuery.VertexMovesReceived);
				//				startNextSuperstep(activeQuery);
			}

			// Flush active vertices
			long startTime = System.currentTimeMillis();
			Map<Integer, Integer> queryIntersects = new HashMap<>();
			{
				ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> swap = activeQuery.ActiveVerticesThis;
				activeQuery.ActiveVerticesThis = activeQuery.ActiveVerticesNext;
				activeQuery.ActiveVerticesNext = swap;
				activeQuery.ActiveVerticesNext.clear();

				// Prepare active vertices
				for (AbstractVertex<V, E, M, Q> vert : activeQuery.ActiveVerticesThis.values()) {
					vert.prepareForNextSuperstep(activeQuery.Query.QueryId);
				}

				// Reset active vertices
				activeQuery.QueryLocal.setActiveVertices(activeQuery.ActiveVerticesThis.size());
				activeQuery.ChannelBarrierWaitSet.addAll(otherWorkerIds);
				activeQuery.QueryLocal.Stats.StepFinishTime = System.currentTimeMillis() - startTime;

				// Overlap test
				long startTime2 = System.currentTimeMillis();
				for (WorkerQuery<V, E, M, Q> otherQuery : activeQueries.values()) {
					if (otherQuery.QueryId == activeQuery.QueryId) continue;
					int intersects;
					intersects = MiscUtil.getIntersectCount(activeQuery.ActiveVerticesThis.keySet(),
							otherQuery.ActiveVerticesThis.keySet());
					queryIntersects.put(otherQuery.QueryId, intersects);
				}
				activeQuery.QueryLocal.Stats.IntersectCalcTime = System.currentTimeMillis() - startTime2;
			}

			sendMasterSuperstepFinished(activeQuery, queryIntersects);
			activeQuery.finishedBarrierSync();
			logger.debug(
					"Worker finished barrier " + activeQuery.Query.QueryId + ":" + activeQuery.getCalculatedSuperstepNo() + ". Active: "
							+ activeQuery.QueryLocal.getActiveVertices());
		}
	}


	@Override
	public void onIncomingVertexMessage(VertexMessage<V, E, M, Q> message) {
		receivedVertexMessages.add(message);
	}

	public void handleVertexMessage(VertexMessage<V, E, M, Q> message) {
		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);
		int superstepNo = activeQuery.getCalculatedSuperstepNo();

		// Discover vertices if enabled. Only discover for broadcast messages as they are a sign that vertices are unknown.
		if (message.broadcastFlag && Settings.VERTEX_MACHINE_DISCOVERY) {
			// Collect all vertices from this broadcast message on this machine
			final HashSet<Integer> srcVertices = new HashSet<>();
			for (final Pair<Integer, M> msg : message.vertexMessages) {
				if (localVertices.containsKey(msg.first)) {
					srcVertices.add(msg.first);
				}
			}
			// Also discover all source vertices from this incoming broadcast message, if enabled.
			if (Settings.VERTEX_MACHINE_DISCOVERY_INCOMING) {
				for (final Integer srcVertId : srcVertices) {
					if (remoteVertexMachineRegistry.addEntry(srcVertId, message.srcMachine))
						activeQuery.QueryLocal.Stats.DiscoveredNewVertexMachines++;
				}
			}
			// Send get-to-know message for all own vertices in this broadcast message
			// Sender did not known these vertices, thats why a broadcast was sent.
			if (!srcVertices.isEmpty())
				messaging.sendGetToKnownMessage(message.srcMachine, srcVertices, message.queryId);
		}

		// If message for correct superstep: Find vertices for them
		if (message.superstepNo < superstepNo) {
			logger.error("Message from past superstep in superstep " + superstepNo + " from " + message.srcMachine);
		}
		else {
			{
				for (final Pair<Integer, M> msg : message.vertexMessages) {
					final AbstractVertex<V, E, M, Q> msgVert = localVertices.get(msg.first);
					if (msgVert != null) {
						activeQuery.QueryLocal.Stats.MessagesReceivedCorrectVertex++;
						List<M> queryInMsgs = msgVert.queryMessagesNextSuperstep.get(message.queryId);
						if (queryInMsgs == null) {
							// Add queue if not already added
							queryInMsgs = new ArrayList<>();
							msgVert.queryMessagesNextSuperstep.put(message.queryId, queryInMsgs);
						}
						queryInMsgs.add(msg.second);
						// Activate vertex
						activeQuery.ActiveVerticesNext.put(msgVert.ID, msgVert);
					}
					else {
						if (!message.broadcastFlag)
							logger.warn("Received non-broadcast vertex message for wrong vertex " + msg.first + " from "
									+ message.srcMachine);
						activeQuery.QueryLocal.Stats.MessagesReceivedWrongVertex++;
					}
				}
			}
		}
	}

	@Override
	public void onIncomingGetToKnowMessage(GetToKnowMessage message) {
		receivedGetToKnowMessages.add(message);
	}

	public void handleGetToKnowMessage(GetToKnowMessage message) {
		for (final Integer srcVertex : message.vertices) {
			if (remoteVertexMachineRegistry.addEntry(srcVertex, message.srcMachine))
				activeQueries.get(message.queryId).QueryLocal.Stats.DiscoveredNewVertexMachines++;
		}
	}

	@Override
	public void onIncomingMoveVerticesMessage(MoveVerticesMessage<V, E, M, Q> message) {
		receivedMoveVerticesMessages.add(message);
	}

	public void handleMoveVerticesMessage(MoveVerticesMessage<V, E, M, Q> message) {
		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);

		{
			for (AbstractVertex<V, E, M, Q> movedVert : message.vertices) {
				activeQuery.ActiveVerticesThis.put(movedVert.ID, movedVert);
				activeQuery.MovedVertices.add(movedVert);
			}

			if (message.lastSegment) {
				// Mark that received all vertices from machine. Start next superstep if ready
				assert !activeQuery.VertexMovesReceived.contains(message.srcMachine);
				activeQuery.VertexMovesReceived.add(message.srcMachine);

				// Start superstep if already received master start next superstep and all vertices received
				if (activeQuery.VertexMovesReceived.size() >= activeQuery.VertexMovesWaitingFor.size() &&
						activeQuery.getPreparedSuperstepNo() == (activeQuery.getCalculatedSuperstepNo() + 1)) {
					// All vertex moves received - start superstep
					assert activeQuery.VertexMovesReceived.containsAll(activeQuery.VertexMovesWaitingFor);
					startNextSuperstep(activeQuery);
				}
			}
		}
	}

	@Override
	public void onIncomingInvalidateRegisteredVerticesMessage(InvalidateRegisteredVerticesMessage message) {
		receivedInvVertsMessages.add(message);
	}

	public void handleInvalidateRegisteredVerticesMessage(InvalidateRegisteredVerticesMessage message) {
		// TODO
		System.out.println("TODO onIncomingInvalidateRegisteredVerticesMessage");
	}


	private void startQuery(Q query) {
		if (activeQueries.containsKey(query.QueryId))
			throw new RuntimeException("Thready with this ID already active: " + query.QueryId);
		WorkerQuery<V, E, M, Q> activeQuery = new WorkerQuery<>(query, globalValueFactory, localVertices.keySet());
		activeQuery.ChannelBarrierWaitSet.addAll(otherWorkerIds);

		activeQueries.put(query.QueryId, activeQuery);

		logger.info("Worker started query " + query.QueryId);
	}

	private void finishQuery(WorkerQuery<V, E, M, Q> activeQuery) {
		new VertexTextOutputWriter<V, E, M, Q>().writeOutput(
				outputDir + File.separator + activeQuery.Query.QueryId + File.separator + ownId + ".txt", localVertices.values(),
				activeQuery.Query.QueryId);
		sendMasterQueryFinishedMessage(activeQuery);
		for (final AbstractVertex<V, E, M, Q> vertex : localVertices.values()) {
			vertex.finishQuery(activeQuery.Query.QueryId);
		}
		logger.info("Worker finished query " + activeQuery.Query.QueryId);
		activeQueries.remove(activeQuery.Query.QueryId);
	}


	@Override
	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}

	private Q deserializeQuery(ByteString bytes) {
		return globalValueFactory.createFromBytes(ByteBuffer.wrap(bytes.toByteArray()));
	}
}
