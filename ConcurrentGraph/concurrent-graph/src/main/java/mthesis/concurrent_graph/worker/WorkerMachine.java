package mthesis.concurrent_graph.worker;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.GetToKnowMessage;
import mthesis.concurrent_graph.communication.Messages;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.MoveVerticesMessage;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.communication.UpdateRegisteredVerticesMessage;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.communication.VertexMessageBucket;
import mthesis.concurrent_graph.util.MiscUtil;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

/**
 * Concurrent graph processing worker main class.
 * Processes active queries. Receives messages and processes the message queue.
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
	private final Int2ObjectMap<AbstractVertex<V, E, M, Q>> localVertices = new Int2ObjectOpenHashMap<>();

	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Global, aggregated QueryGlobalValues. Aggregated and sent by master
	private final Map<Integer, WorkerQuery<V, E, M, Q>> activeQueries = new HashMap<>();
	private final List<WorkerQuery<V, E, M, Q>> activeQueriesThisSuperstep = new ArrayList<>();

	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	// Buckets to collect messages to send
	private final VertexMessageBucket<M> vertexMessageBroadcastBucket = new VertexMessageBucket<>();
	private final Map<Integer, VertexMessageBucket<M>> vertexMessageMachineBuckets = new HashMap<>();

	private final ConcurrentLinkedQueue<ChannelMessage> receivedMessages = new ConcurrentLinkedQueue<>();

	// Most recent calculated query intersections
	private Map<Integer, Map<Integer, Integer>> currentQueryIntersects = new HashMap<>();

	// Vertex redirections for vertex moved away from this machine
	private final Map<Integer, Integer> movedVerticesRedirections = new HashMap<>();
	private final Map<Integer, List<Integer>> movedQueryVertices = new HashMap<>();

	// Global barrier coordination/control
	private boolean globalBarrierRequested = false;// TODO Enum?
	private final Set<Integer> globalBarrierStartWaitSet = new HashSet<>();
	private final Set<Integer> globalBarrierFinishWaitSet = new HashSet<>();
	// Global barrier commands to perform while barrier
	private List<Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage> globalBarrierSendVerts;
	private Set<Pair<Integer, Integer>> globalBarrierRecvVerts;

	// Worker stats
	private static final long workerStatsInverval = 1000;
	private long masterStartTime;
	private long workerStatsLastSample = System.currentTimeMillis();
	// Current worker stats
	private WorkerStats workerStats = new WorkerStats();
	// Samples that are finished but not sent to master yet
	private List<WorkerStatSample> workerStatsSamplesToSend = new ArrayList<>();


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
			logger.debug("Worker loaded partitions");
			System.gc();
			logger.debug("Worker pre-partition-load GC finished");
			sendMasterInitialized();

			// Execution loop
			while (!Thread.interrupted()) {
				// Wait for queries
				long startTime = System.nanoTime();
				while (activeQueries.isEmpty()) {
					Thread.sleep(1); // TODO sleep?
					handleReceivedMessages();
				}
				workerStats.IdleTime += (System.nanoTime() - startTime);

				// Global barrier if requested
				if (globalBarrierRequested) {
					// Start barrier, notify other workers
					logger.debug("Barrier started, waiting for other workers to start");
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Started(ownId),
							true);
					// wait for other workers barriers
					startTime = System.nanoTime();
					while (!globalBarrierStartWaitSet.isEmpty()) {
						//Thread.sleep(1);  // TODO sleep?
						handleReceivedMessages();
					}
					long barrierStartWaitTime = System.nanoTime() - startTime;

					// Barrier tasks
					startTime = System.nanoTime();
					// Send vertices
					for (SendQueryVerticesMessage sendVert : globalBarrierSendVerts) {
						sendQueryVerticesToMove(sendVert.getQueryId(), sendVert.getMoveToMachine(),
								sendVert.getMaxMoveCount());
					}
					// Receive vertices
					while (!globalBarrierRecvVerts.isEmpty()) {
						//Thread.sleep(1);  // TODO sleep?
						handleReceivedMessages();
					}
					workerStats.BarrierVertexMoveTime += (System.nanoTime() - startTime);

					// Start barrier, notify other workers
					logger.debug("Barrier tasks done, waiting for other workers to finish");
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Finished(ownId), true);
					startTime = System.nanoTime();
					while (!globalBarrierFinishWaitSet.isEmpty()) {
						//Thread.sleep(1);  // TODO sleep?
						handleReceivedMessages();
					}
					long barrierFinishWaitTime = System.nanoTime() - startTime;

					logger.debug("Barrier finished");
					// TODO Remove sysout, barrier times as worker stats
					//					System.out.println(ownId + " " +
					//							(double) barrierStartWaitTime / 1000000 + " " + (double) barrierFinishWaitTime / 1000000);
					workerStats.BarrierStartWaitTime += barrierStartWaitTime;
					workerStats.BarrierFinishWaitTime += barrierFinishWaitTime;
					globalBarrierRequested = false;
				}


				// Wait for ready queries
				startTime = System.nanoTime();
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
				workerStats.QueryWaitTime += (System.nanoTime() - startTime);


				// Compute active queries
				for (WorkerQuery<V, E, M, Q> activeQuery : activeQueriesThisSuperstep) {
					{
						int queryId = activeQuery.Query.QueryId;
						int superstepNo = activeQuery.getStartedSuperstepNo();

						// Next superstep. Compute and Messaging (done by vertices)
						logger.trace("Worker start query " + queryId + " superstep compute " + superstepNo);

						// First frame: Call all vertices, second frame only active vertices
						startTime = System.nanoTime();
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
						long computeTime = System.nanoTime() - startTime;
						activeQuery.QueryLocal.Stats.OtherStats.put(QueryStats.ComputeTimeKey, computeTime);
						workerStats.ComputeTime += computeTime;


						// Barrier sync with other workers;
						if (!otherWorkerIds.isEmpty()) {
							flushVertexMessages(activeQuery);
							sendWorkersSuperstepFinished(activeQuery);
						}
						else {
							checkSuperstepBarrierFinished(activeQuery);
						}
						logger.trace("Worker finished compute " + queryId + ":" + superstepNo);
					}

					// Finish barrier sync if received all barrier syncs from other workers
					if (!otherWorkerIds.isEmpty()) {
						checkSuperstepBarrierFinished(activeQuery);
					}

					// Handle received messages after each query
					handleReceivedMessages();
				}


				// Worker stats
				if ((System.currentTimeMillis() - workerStatsLastSample) >= workerStatsInverval) {
					long activeVertices = 0;
					for (WorkerQuery<V, E, M, Q> query : activeQueries.values())
						activeVertices += query.ActiveVerticesThis.size();

					workerStats.ActiveVertices = activeVertices;
					workerStatsSamplesToSend
							.add(workerStats.getSample(System.currentTimeMillis() - masterStartTime));
					workerStatsLastSample = System.currentTimeMillis();
					workerStats = new WorkerStats();
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
	@SuppressWarnings("unchecked")
	private void handleReceivedMessages() {
		long startTime = System.nanoTime();
		ChannelMessage message;
		boolean interruptedMsgHandling = false;
		while (!interruptedMsgHandling && (message = receivedMessages.poll()) != null) {
			switch (message.getTypeCode()) {
				case 0:
					handleVertexMessage((VertexMessage<V, E, M, Q>) message);
					break;
				case 1:
					MessageEnvelope msgEnvelope = ((ProtoEnvelopeMessage) message).message;
					if (msgEnvelope.hasControlMessage()) {
						interruptedMsgHandling = handleControlMessage(msgEnvelope.getControlMessage());
					}
					break;
				case 2:
					handleGetToKnowMessage((GetToKnowMessage) message);
					break;
				case 3:
					handleMoveVerticesMessage((MoveVerticesMessage<V, E, M, Q>) message);
					break;
				case 4:
					handleUpdateRegisteredVerticesMessage((UpdateRegisteredVerticesMessage) message);
					break;

				default:
					logger.warn("Unknown incoming message id: " + message.getTypeCode());
					break;
			}
		}
		workerStats.HandleMessagesTime += (System.nanoTime() - startTime);
	}


	public void flushVertexMessages(WorkerQuery<V, E, M, Q> query) {
		sendBroadcastVertexMessageBucket(query, query.getStartedSuperstepNo());
		for (final int otherWorkerId : otherWorkerIds) {
			final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(otherWorkerId);
			if (!msgBucket.messages.isEmpty())
				sendUnicastVertexMessageBucket(msgBucket, otherWorkerId, query, query.getStartedSuperstepNo());
			messaging.flushAsyncChannel(otherWorkerId);
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
						workerQuery.QueryLocal, queryIntersects, workerStatsSamplesToSend),
				true);
		workerStatsSamplesToSend.clear();
	}

	private void sendMasterQueryFinishedMessage(WorkerQuery<V, E, M, Q> workerQuery) {
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QueryFinished(workerQuery.getStartedSuperstepNo(), ownId, workerQuery.QueryLocal),
				true);
	}



	@Override
	public M getPooledMessageValue() {
		return jobConfig.getPooledMessageValue();
	}

	@Override
	public void freePooledMessageValue(M message) {
		jobConfig.freePooledMessageValue(message);
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
				sendVertexMessageToMachine(remoteMachine, dstVertex, query, query.getStartedSuperstepNo(), messageContent);
			}
			else {
				// Broadcast remote message
				query.QueryLocal.Stats.MessagesSentBroadcast += otherWorkerIds.size();
				vertexMessageBroadcastBucket.addMessage(dstVertex, messageContent);
				if (vertexMessageBroadcastBucket.messages.size() > Configuration.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
					sendBroadcastVertexMessageBucket(query, query.getStartedSuperstepNo());
				}
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int dstMachine, int dstVertex, WorkerQuery<V, E, M, Q> query, int superstepNo,
			M messageContent) {
		final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(dstMachine);
		if (msgBucket == null)
			logger.error("WTF, no machine " + dstMachine);
		msgBucket.addMessage(dstVertex, messageContent);
		query.QueryLocal.Stats.MessagesSentUnicast++;
		if (msgBucket.messages.size() > Configuration.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
			sendUnicastVertexMessageBucket(msgBucket, dstMachine, query, superstepNo);
		}
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
		final List<Pair<Integer, M>> msgList = new ArrayList<>(msgBucket.messages); // Could Pool instance
		msgBucket.messages.clear();
		return msgList;
	}


	@Override
	public void onIncomingMessage(ChannelMessage message) {
		receivedMessages.add(message);
	}


	/**
	 * Handles an incoming control message
	 * @param message
	 * @return Returns true if message handling should be interrupted after this message
	 */
	public boolean handleControlMessage(ControlMessage message) {
		try {
			if (message != null) {
				switch (message.getType()) {
					case Master_Worker_Initialize:
						assignedPartitions = message.getAssignPartitions().getPartitionFilesList();
						masterStartTime = message.getAssignPartitions().getMasterStartTime();
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

					case Master_Start_Barrier: {
						globalBarrierStartWaitSet.addAll(otherWorkerIds);
						globalBarrierFinishWaitSet.addAll(otherWorkerIds);
						globalBarrierSendVerts = message.getStartBarrier().getSendQueryVerticesList();
						List<ReceiveQueryVerticesMessage> recvVerts = message.getStartBarrier().getReceiveQueryVerticesList();
						globalBarrierRecvVerts = new HashSet<>(recvVerts.size());
						for (ReceiveQueryVerticesMessage rvMsg : recvVerts) {
							globalBarrierRecvVerts
									.add(new Pair<Integer, Integer>(rvMsg.getQueryId(), rvMsg.getReceiveFromMachine()));
						}
						globalBarrierRequested = true;
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
						return true;

					case Worker_Barrier_Started: {
						System.out.println("Worker_Barrier_Started");
						int srcWorker = message.getSrcMachine();
						if (globalBarrierStartWaitSet.contains(srcWorker)) globalBarrierStartWaitSet.remove(srcWorker);
						else logger.warn("Worker_Barrier_Started message from worker not waiting for: " + message);
					}
						return true;
					case Worker_Barrier_Finished: {
						System.out.println("Worker_Barrier_Finished");
						int srcWorker = message.getSrcMachine();
						if (globalBarrierFinishWaitSet.contains(srcWorker))
							globalBarrierFinishWaitSet.remove(srcWorker);
						else logger.warn("Worker_Barrier_Started message from worker not waiting for: " + message);
					}
						return true;

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
			return false;
		}
		catch (Exception e) {
			logger.error("exception at incomingControlMessage", e);
		}
		return false;
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
				// TODO Live move will not work anymore
				throw new RuntimeException("LiveMove not implemented");
				//				// Send query vertices to other worker
				//				int sendTo = message.getSendQueryVertices().getSendToMachine();
				//				sendQueryVerticesToMove(activeQuery, sendTo);
			}
			else if (message.hasReceiveQueryVertices()) {
				// TODO Live move will not work anymore
				throw new RuntimeException("LiveMove not implemented");
				//				List<Integer> recvFrom = message.getReceiveQueryVertices().getRecvFromMachineList();
				//				if (!recvFrom.isEmpty()) {
				//					// Wait for receiving vertices from other worker
				//					assert activeQuery.VertexMovesWaitingFor.isEmpty();
				//					activeQuery.VertexMovesWaitingFor.addAll(recvFrom);
				//				}
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
	private void sendQueryVerticesToMove(int queryId, int sendToWorker, int maxVertices) {
		long startTime = System.nanoTime();
		int verticesSent = 0;
		int verticesMessagesSent = 0;
		WorkerQuery<V, E, M, Q> query = activeQueries.get(queryId);
		List<AbstractVertex<V, E, M, Q>> verticesToMove = new ArrayList<>();

		if (movedQueryVertices.containsKey(queryId))
			logger.error("movedVerticesRedirections not cleaned up after last superstep");

		if (query != null) {
			//			if (getQueryIntersectionCount(queryId) == 0) { // Only move vertices if no intersection
			for (AbstractVertex<V, E, M, Q> vertex : query.ActiveVerticesThis.values()) {
				localVertices.remove(vertex.ID);
				verticesToMove.add(vertex);
				// Send vertices now if bucket full
				if (verticesToMove.size() >= Configuration.VERTEX_MOVE_BUCKET_MAX_VERTICES) {
					verticesMoving(verticesToMove, query.QueryId, sendToWorker);
					messaging.sendMoveVerticesMessage(sendToWorker, verticesToMove, queryId, false);
					verticesSent += verticesToMove.size();
					verticesToMove = new ArrayList<>();
				}
				if (Configuration.DETAILED_STATS) {
					verticesMessagesSent += vertex.getBufferedMessageCount();
				}
			}
			System.out.println(ownId + " Sent " + query.ActiveVerticesThis.size() + " for " + queryId);
			query.ActiveVerticesThis.clear();
			//			}
			//			else {
			//				logger.info(queryId + " vertices not moved because has intersection now: "
			//						+ currentQueryIntersects.get(queryId));
			//			}
		}
		else {
			logger.info("Vertices not moved because query inactive: " + queryId);
		}

		// Send remaining vertices (or empty message if none)
		verticesMoving(verticesToMove, queryId, sendToWorker);
		messaging.sendMoveVerticesMessage(sendToWorker, verticesToMove, queryId, true);
		verticesSent += verticesToMove.size();

		long sendTime = (System.nanoTime() - startTime);
		if (query != null) {
			query.QueryLocal.Stats.addToOtherStat(QueryStats.MoveSendVerticsKey, verticesSent);
			query.QueryLocal.Stats.addToOtherStat(QueryStats.MoveSendVerticsTimeKey, sendTime);
		}
		workerStats.MoveSendVertices += verticesSent;
		workerStats.MoveSendVerticesTime += sendTime;
		workerStats.MoveSendVerticesMessages += verticesMessagesSent;
	}


	/**
	 * Handling for vertices which are moved
	 */
	private void verticesMoving(List<AbstractVertex<V, E, M, Q>> verticesMoving, int queryId, int movedTo) {
		if (verticesMoving.isEmpty()) return;
		List<Integer> vertexMoveIds = verticesMoving.stream().map(v -> v.ID)
				.collect(Collectors.toCollection(ArrayList::new));

		// Add vertex redirections
		movedQueryVertices.put(queryId, vertexMoveIds);
		for (Integer movedVert : vertexMoveIds) {
			movedVerticesRedirections.put(movedVert, movedTo);
		}

		// Broadcast vertex invalidate message
		for (int otherWorker : otherWorkerIds) {
			messaging.sendInvalidateRegisteredVerticesMessage(otherWorker, vertexMoveIds, movedTo, queryId);
		}

		// Remove vertices registry entries
		remoteVertexMachineRegistry.removeEntries(vertexMoveIds);
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
			long startTime = System.nanoTime();

			ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> swap = activeQuery.ActiveVerticesThis;
			activeQuery.ActiveVerticesThis = activeQuery.ActiveVerticesNext;
			activeQuery.ActiveVerticesNext = swap;
			activeQuery.ActiveVerticesNext.clear();

			// Prepare active vertices
			Integer queryId = activeQuery.Query.QueryId;
			for (AbstractVertex<V, E, M, Q> vert : activeQuery.ActiveVerticesThis.values()) {
				vert.prepareForNextSuperstep(queryId);
			}

			// Reset active vertices
			activeQuery.QueryLocal.setActiveVertices(activeQuery.ActiveVerticesThis.size());
			activeQuery.ChannelBarrierWaitSet.addAll(otherWorkerIds);
			activeQuery.QueryLocal.Stats.OtherStats.put(QueryStats.StepFinishTimeKey, System.nanoTime() - startTime);


			// Calculate query intersections
			Map<Integer, Integer> queryIntersects = new HashMap<>();
			long startTime2 = System.nanoTime();
			for (WorkerQuery<V, E, M, Q> otherQuery : activeQueries.values()) {
				if (otherQuery.QueryId == activeQuery.QueryId) continue;
				int intersects;
				intersects = MiscUtil.getIntersectCount(activeQuery.ActiveVerticesThis.keySet(),
						otherQuery.ActiveVerticesThis.keySet());
				queryIntersects.put(otherQuery.QueryId, intersects);
			}
			activeQuery.QueryLocal.Stats.OtherStats.put(QueryStats.IntersectCalcTimeKey, System.nanoTime() - startTime2);

			// Update currentQueryIntersects
			currentQueryIntersects.put(activeQuery.QueryId, queryIntersects);
			for (Entry<Integer, Integer> intersection : queryIntersects.entrySet()) {
				Map<Integer, Integer> otherIntersects = currentQueryIntersects.get(intersection.getKey());
				if (otherIntersects == null) {
					otherIntersects = new HashMap<>();
					currentQueryIntersects.put(intersection.getKey(), otherIntersects);
				}
				otherIntersects.put(activeQuery.QueryId, intersection.getValue());
			}


			// Clean up redirections
			List<Integer> redirections = movedQueryVertices.get(activeQuery.QueryId);
			if (redirections != null) {
				for (Integer redir : redirections) {
					movedVerticesRedirections.remove(redir);
				}
				movedQueryVertices.remove(activeQuery.QueryId);
			}


			sendMasterSuperstepFinished(activeQuery, queryIntersects);
			activeQuery.finishedBarrierSync();
			logger.trace(
					"Worker finished barrier " + activeQuery.Query.QueryId + ":" + activeQuery.getCalculatedSuperstepNo() + ". Active: "
							+ activeQuery.QueryLocal.getActiveVertices());
		}
	}


	public void handleVertexMessage(VertexMessage<V, E, M, Q> message) {
		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);
		int superstepNo = activeQuery.getCalculatedSuperstepNo();
		int barrierSuperstepNo = activeQuery.getBarrierFinishedSuperstepNo();

		// Discover vertices if enabled. Only discover for broadcast messages as they are a sign that vertices are unknown.
		if (message.broadcastFlag && Configuration.VERTEX_MACHINE_DISCOVERY) {
			// Collect all vertices from this broadcast message on this machine
			final HashSet<Integer> srcVertices = new HashSet<>();
			for (final Pair<Integer, M> msg : message.vertexMessages) {
				if (localVertices.containsKey(msg.first)) {
					srcVertices.add(msg.first);
				}
			}
			// Also discover all source vertices from this incoming broadcast message, if enabled.
			if (Configuration.VERTEX_MACHINE_DISCOVERY_INCOMING) {
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

		if (message.superstepNo != superstepNo && message.superstepNo != (superstepNo + 1)) {
			logger.error(
					"VertexMessage not from this or next calc superstepNo: " + message.superstepNo + " during " + superstepNo + " from "
							+ message.srcMachine);
		}
		else if (message.superstepNo != (barrierSuperstepNo + 1)) {
			logger.error(
					"VertexMessage from wrong barrier superstepNo: " + message.superstepNo + " should be " + (barrierSuperstepNo + 1)
							+ " from " + message.srcMachine);
		}
		else {
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
					if (!message.broadcastFlag) {
						Integer redirectMachine = movedVerticesRedirections.get(msg.first);
						if (redirectMachine != null) {
							//								logger.info("Redirect to " + redirectMachine);
							activeQuery.QueryLocal.Stats.addToOtherStat(QueryStats.RedirectedMessagesKey, 1);
							sendVertexMessageToMachine(redirectMachine, msg.first, activeQuery, message.superstepNo, msg.second);
						}
						else {
							logger.warn("Received non-broadcast vertex message for wrong vertex " + msg.first + " from "
									+ message.srcMachine + " query " + activeQuery.QueryId + ":" + superstepNo
									+ " with no redirection");
							//								Integer machineMb = remoteVertexMachineRegistry.lookupEntry(msg.first);
							//								System.err.println(machineMb);
							freePooledMessageValue(msg.second);
						}
					}
					else {
						freePooledMessageValue(msg.second);
					}
					activeQuery.QueryLocal.Stats.MessagesReceivedWrongVertex++;
				}
			}
		}
		message.free(false);
	}


	public void handleGetToKnowMessage(GetToKnowMessage message) {
		for (final Integer srcVertex : message.vertices) {
			if (remoteVertexMachineRegistry.addEntry(srcVertex, message.srcMachine))
				activeQueries.get(message.queryId).QueryLocal.Stats.DiscoveredNewVertexMachines++;
		}
	}


	public void handleMoveVerticesMessage(MoveVerticesMessage<V, E, M, Q> message) {
		long startTime = System.nanoTime();

		if (message.lastSegment) {
			// Remove from globalBarrierRecvVerts if received all vertices
			globalBarrierRecvVerts.remove(new Pair<>(message.queryId, message.srcMachine));
		}

		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);
		if (activeQuery == null)
			return;

		for (AbstractVertex<V, E, M, Q> movedVert : message.vertices) {
			activeQuery.ActiveVerticesThis.put(movedVert.ID, movedVert);
			localVertices.put(movedVert.ID, movedVert);
		}

		// Testcode
		//			StringBuilder movedSb = new StringBuilder();
		//			for (AbstractVertex<V, E, M, Q> movedVert : message.vertices) {
		//				movedSb.append(movedVert.ID);
		//				movedSb.append(",");
		//			}
		//			System.out.println(ownId + " rec " + movedSb.toString());

		if (message.lastSegment) {
			// Mark that received all vertices from machine. Start next superstep if ready
			assert !activeQuery.VertexMovesReceived.contains(message.srcMachine);
			activeQuery.VertexMovesReceived.add(message.srcMachine);

			// TODO Live vertex move will not work anymore
			//			// Start superstep if already received master start next superstep and all vertices received
			//			if (activeQuery.VertexMovesReceived.size() >= activeQuery.VertexMovesWaitingFor.size() &&
			//					activeQuery.getPreparedSuperstepNo() == (activeQuery.getCalculatedSuperstepNo() + 1)) {
			//				// All vertex moves received - start superstep
			//				assert activeQuery.VertexMovesReceived.containsAll(activeQuery.VertexMovesWaitingFor);
			//				startNextSuperstep(activeQuery);
			//			}
		}

		long moveTime = (System.nanoTime() - startTime);
		workerStats.MoveRecvVertices += message.vertices.size();
		workerStats.MoveSendVerticesTime += moveTime;
		activeQuery.QueryLocal.Stats.addToOtherStat(QueryStats.MoveRecvVerticsKey, message.vertices.size());
		activeQuery.QueryLocal.Stats.addToOtherStat(QueryStats.MoveRecvVerticsTimeKey, moveTime);
	}


	public void handleUpdateRegisteredVerticesMessage(UpdateRegisteredVerticesMessage message) {
		WorkerQuery<V, E, M, Q> query = activeQueries.get(message.queryId);
		assert (query != null);
		int updatedEntries;
		if (message.movedTo != ownId)
			updatedEntries = remoteVertexMachineRegistry.updateEntries(message.vertices, message.movedTo);
		else
			updatedEntries = remoteVertexMachineRegistry.removeEntries(message.vertices);
		query.QueryLocal.Stats.addToOtherStat(QueryStats.UpdateVertexRegistersKey, updatedEntries);
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

		if (movedQueryVertices.containsKey(activeQuery.QueryId))
			logger.error("movedVerticesRedirections not cleaned up after last superstep");
	}


	@Override
	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}

	private Q deserializeQuery(ByteString bytes) {
		return globalValueFactory.createFromBytes(ByteBuffer.wrap(bytes.toByteArray()));
	}


	//	/**
	//	 * @return number of active vertices of this query intersecting with other queries.
	//	 */
	//	private int getQueryIntersectionCount(int queryId) {
	//		int totalIntersects = 0;
	//		Map<Integer, Integer> intersects = currentQueryIntersects.get(queryId);
	//		if (intersects != null) {
	//			for (Integer qInters : intersects.values()) {
	//				totalIntersects += qInters;
	//			}
	//		}
	//		return totalIntersects;
	//	}
}
