package mthesis.concurrent_graph.worker;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.VertexMessageBucket;
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
		extends AbstractMachine<M> implements VertexWorkerInterface<V, E, M, Q> {

	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String outputDir;

	private final JobConfiguration<V, E, M, Q> jobConfig;

	private List<AbstractVertex<V, E, M, Q>> localVerticesList;
	private final Map<Integer, AbstractVertex<V, E, M, Q>> localVerticesIdMap = new HashMap<>();

	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Global, aggregated QueryGlobalValues. Aggregated and sent by master
	private final Map<Integer, Q> activeQueries = new HashMap<>();
	// Local QueryGlobalValues, are sent to master for aggregation.
	private final Map<Integer, Q> activeQueriesLocal = new HashMap<>();

	private final Map<Integer, Set<Integer>> queryChannelBarrierWaitSet = new HashMap<>();
	private final Map<Integer, Map<Integer, List<M>>> queryInVertexMessages = new HashMap<>();
	private final Map<Integer, Integer> querySuperstepNos = new HashMap<>();

	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	// Buckets to collect messages to send
	private final VertexMessageBucket<M> vertexMessageBroadcastBucket = new VertexMessageBucket<>();
	private final Map<Integer, VertexMessageBucket<M>> vertexMessageMachineBuckets = new HashMap<>();
	// Buffer for sending outgoing message buckets
	//	private final int[] outVertexMsgDstIdBuffer = new int[Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES];
	//	private final List<M> outVertexMsgContentBuffer = new ArrayList<>(Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES);

	// TODO re-introduce superstepStats?
	//private SuperstepStats superstepStats;



	public WorkerMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, int masterId, String outputDir,
			JobConfiguration<V, E, M, Q> jobConfig) {
		super(machines, ownId, jobConfig.getMessageValueFactory());
		this.masterId = masterId;
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		for (final Integer workerId : otherWorkerIds) {
			vertexMessageMachineBuckets.put(workerId, new VertexMessageBucket<>());
		}
		vertexValueFactory = jobConfig.getVertexValueFactory();
		globalValueFactory = jobConfig.getGlobalValuesFactory();

		this.outputDir = outputDir;
		this.jobConfig = jobConfig;
	}

	private void loadVertices(List<String> partitions) {
		localVerticesList = new VertexTextInputReader<V, E, M, Q>().getVertices(partitions, jobConfig, this);
	}



	@Override
	public void run() {
		logger.info("Starting run worker node " + ownId);
		//superstepStats = new SuperstepStats();

		// Wait for master to signal that input ready
		final List<String> assignedPartitions = waitForStartup();
		if (assignedPartitions == null) {
			logger.error("Wait for input ready failed");
			return;
		}

		// Initialize, load assigned partitions
		loadVertices(assignedPartitions);
		sendMasterInitialized();

		try {
			// Execution loop
			while (!Thread.interrupted()) {
				// Wait for queries
				while (activeQueries.isEmpty()) {
					Thread.sleep(100);
				}

				for (Q activeQuery : activeQueries.values()) {
					int queryId = activeQuery.QueryId;
					int superstepNo = querySuperstepNos.get(activeQuery.QueryId);

					// Next superstep
					queryChannelBarrierWaitSet.get(queryId).addAll(otherWorkerIds);
					//superstepStats = new SuperstepStats();
					logger.debug("Starting query " + queryId + " superstep " + superstepNo);

					// Compute and Messaging (done by vertices)
					logger.info("Worker start query " + queryId + " superstep compute " + superstepNo);
					for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
						//final List<VertexMessage<M>> vertMsgs = vertexMessageBuckets.get(vertex.ID);
						vertex.superstep(superstepNo, activeQuery);
						//vertMsgs.clear();
					}
					logger.debug("Worker finished query " + queryId + " superstep compute " + superstepNo);


					// Barrier sync with other workers;
					flushVertexMessages(queryId);
					sendWorkersSuperstepFinished(activeQuery);
					waitForWorkerSuperstepsFinished(queryId);
					logger.debug("Worker finished query " + queryId + " superstep barrier " + superstepNo);


					// Sort messages from buffers after barrier sync
					// Incoming messages
					synchronized (queryInVertexMessages) {
						Map<Integer, List<M>> inVertexMessages = queryInVertexMessages.get(queryId);
						for (final Entry<Integer, List<M>> msgQueue : inVertexMessages.entrySet()) {
							final AbstractVertex<V, E, M, Q> vertex = localVerticesIdMap.get(msgQueue.getKey());
							if (vertex != null) {
								if (!msgQueue.getValue().isEmpty())
									vertex.queryMessagesNextSuperstep.get(activeQuery.QueryId).addAll(msgQueue.getValue());
							}
							else {
								logger.error("Cannot find vertex for messages: " + msgQueue.getKey());
							}
							msgQueue.getValue().clear();
						}
					}

					// Count active vertices
					int activeVertices = 0;
					for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
						if (vertex.isActive(activeQuery.QueryId)) activeVertices++;
					}
					Q queryLocal = activeQueriesLocal.get(queryId);
					queryLocal.setActiveVertices(activeVertices);
					logger.debug("Worker finished query " + queryId + " superstep message sort " + superstepNo + " activeVertices: "
							+ activeVertices);

					// Signal master that ready
					//superstepStats.TotalVertexMachinesDiscovered = remoteVertexMachineRegistry.getRegistrySize();
					sendMasterSuperstepFinished(queryLocal);


					// Wait for start superstep from master
					if (!waitForMasterNextSuperstep(queryId)) {
						new VertexTextOutputWriter<V, E, M, Q>().writeOutput(
								outputDir + File.separator + activeQuery.QueryId + File.separator + ownId + ".txt", localVerticesList,
								activeQuery.QueryId);
						sendMasterQueryFinishedMessage(queryLocal);
						for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
							vertex.finishQuery(activeQuery.QueryId);
						}
						logger.info("Worker finished query " + activeQuery.QueryId);
						activeQuery = null;
						activeQueries.remove(queryId);
						activeQueriesLocal.remove(queryId);
						break;
					}


					querySuperstepNos.put(queryId, superstepNo + 1);
				}
			}
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


	public void flushVertexMessages(int queryId) {
		sendBroadcastVertexMessageBucket(queryId);
		for (final int otherWorkerId : otherWorkerIds) {
			final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(otherWorkerId);
			if (!msgBucket.messages.isEmpty()) sendUnicastVertexMessageBucket(msgBucket, otherWorkerId, queryId);
			messaging.flushChannel(otherWorkerId);
		}
	}

	public boolean waitForWorkerSuperstepsFinished(int queryId) {
		int superstepNo = querySuperstepNos.get(queryId);
		try {
			while (!Thread.interrupted() && !queryChannelBarrierWaitSet.get(queryId).isEmpty()) {
				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
				if (msg != null) {
					switch (msg.getType()) {
						case Worker_Query_Superstep_Barrier:
							if (msg.getSuperstepNo() == superstepNo) {
								queryChannelBarrierWaitSet.get(queryId).remove(msg.getSrcMachine());
							}
							else {
								logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: " + msg.getSuperstepNo()
										+ " at step " + superstepNo);
							}
							break;

						default:
							logger.error("Illegal control while waitForWorkerSuperstepsFinished: " + msg.getType());
							break;
					}
				}
				else {
					logger.error("Timeout while waitForWorkerSuperstepsFinished");
					return false;
				}
			}
			return queryChannelBarrierWaitSet.get(queryId).isEmpty();
		}
		catch (final InterruptedException e) {
			return false;
		}
	}

	// Waits for startup, returns assigned partitions
	public List<String> waitForStartup() {
		try {
			final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);

			if (msg != null) {
				if (msg.getType() == ControlMessageType.Master_Worker_Initialize) {
					return msg.getAssignPartitions().getPartitionFilesList();
				}
				else {
					logger.error("Illegal control while waitForStartup: " + msg.getType());
					return null;
				}
			}
			else {
				logger.error("Timeout while waitForStartup");
				return null;
			}
		}
		catch (final InterruptedException e) {
			logger.error("Interrupt while waitForStartup");
			return null;
		}
	}

	/**
	 * Waits for master to signal next superstep
	 * @return True if continueing with this query, fals otherwise
	 */
	public boolean waitForMasterNextSuperstep(int queryId) {
		int superstepNo = querySuperstepNos.get(queryId);
		try {
			while (!Thread.interrupted()) {
				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);

				if (msg != null) {
					switch (msg.getType()) {
						case Master_Query_Next_Superstep:
							if (msg.getSuperstepNo() == superstepNo + 1) {
								Q query = globalValueFactory.createFromBytes(ByteBuffer.wrap(msg.getQueryValues().toByteArray()));
								activeQueries.put(queryId, query);
								activeQueriesLocal.put(queryId, globalValueFactory.createClone(query));
								return true;
							}
							else {
								logger.error("Received Master_Next_Superstep with wrong superstepNo: " + msg.getSuperstepNo() + " at step "
										+ superstepNo);
								return false;
							}
						case Master_Query_Finished:
							logger.info("Received Master_Finish");
							return false;
						case Worker_Query_Superstep_Barrier: // Barrier from workers which are finished before we even started
							if (msg.getSuperstepNo() == superstepNo + 1) {
								queryChannelBarrierWaitSet.get(queryId).remove(msg.getSrcMachine());
							}
							else {
								logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: " + msg.getSuperstepNo()
										+ " at wait for " + (superstepNo + 1));
							}
							break;

						default:
							logger.error("Illegal control while waitForMasterNextSuperstep: " + msg.getType());
							return false;
					}
				}
				else {
					logger.error("Timeout while waitForMasterNextSuperstep");
					return false;
				}
			}
			logger.error("Timeout or interrupt while waitForMasterNextSuperstep");
			return false;

		}
		catch (final InterruptedException e) {
			logger.error("Interrupt while waitForMasterNextSuperstep");
			return false;
		}
	}


	private void sendWorkersSuperstepFinished(Q query) {
		int superstepNo = querySuperstepNos.get(query.QueryId);
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageMulticast(otherWorkerIds,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepBarrier(superstepNo, ownId, query), true);
	}

	private void sendMasterInitialized() {
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Initialized(ownId, localVerticesList.size()),
				true);
	}

	private void sendMasterSuperstepFinished(Q queryLocal) {
		int superstepNo = querySuperstepNos.get(queryLocal.QueryId);
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(superstepNo, ownId, queryLocal), true);
		//ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(superstepNo, ownId, superstepStats, queryLocal), true);
	}

	private void sendMasterQueryFinishedMessage(Q queryLocal) {
		int superstepNo = querySuperstepNos.get(queryLocal.QueryId);
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QueryFinished(superstepNo, ownId, queryLocal), true);
	}

	/**
	 * Sends a vertex message. If local vertex, direct loopback. It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	@Override
	public void sendVertexMessage(int dstVertex, M messageContent, int queryId) {

		Map<Integer, List<M>> inMsgs = queryInVertexMessages.get(queryId);
		final List<M> localVertMsgQueue = inMsgs.get(dstVertex);
		if (localVertMsgQueue != null) {
			// Local message
			//superstepStats.SentVertexMessagesLocal++;
			synchronized (queryInVertexMessages) {
				localVertMsgQueue.add(messageContent);
			}
		}
		else {
			// Remote message
			final Integer remoteMachine = remoteVertexMachineRegistry.lookupEntry(dstVertex);
			if (remoteMachine != null) {
				// Unicast remote message
				sendVertexMessageToMachine(remoteMachine, dstVertex, queryId, messageContent);
			}
			else {
				// Broadcast remote message
				//superstepStats.SentVertexMessagesBroadcast += otherWorkerIds.size();
				vertexMessageBroadcastBucket.addMessage(dstVertex, messageContent);
				if (vertexMessageBroadcastBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
					sendBroadcastVertexMessageBucket(queryId);
				}
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int dstMachine, int dstVertex, int queryId, M messageContent) {
		//superstepStats.SentVertexMessagesUnicast++;
		final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(dstMachine);
		msgBucket.addMessage(dstVertex, messageContent);
		if (msgBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
			sendUnicastVertexMessageBucket(msgBucket, dstMachine, queryId);
		}
	}

	private void sendUnicastVertexMessageBucket(VertexMessageBucket<M> msgBucket, int dstMachine, int queryId) {
		int superstepNo = querySuperstepNos.get(queryId);
		final List<Pair<Integer, M>> msgList = packVertexMessage(msgBucket);
		messaging.sendVertexMessageUnicast(dstMachine, superstepNo, ownId, queryId, msgList);
		//superstepStats.SentVertexMessagesBuckets++;
	}

	private void sendBroadcastVertexMessageBucket(int queryId) {
		int superstepNo = querySuperstepNos.get(queryId);
		final List<Pair<Integer, M>> msgList = packVertexMessage(vertexMessageBroadcastBucket);
		messaging.sendVertexMessageBroadcast(otherWorkerIds, superstepNo, ownId, queryId, msgList);
		//superstepStats.SentVertexMessagesBuckets += otherWorkerIds.size();
	}

	private List<Pair<Integer, M>> packVertexMessage(VertexMessageBucket<M> msgBucket) {
		final List<Pair<Integer, M>> msgList = new ArrayList<>(msgBucket.messages); // TODO Pool instance
		msgBucket.messages.clear();
		return msgList;
	}

	// TODO Flush all channels at end of frame



	@Override
	public void onIncomingControlMessage(ControlMessage message) {
		// TODO Everything async
		if (message.getType() == ControlMessageType.Master_Query_Start) {
			startQuery(globalValueFactory.createFromBytes(ByteBuffer.wrap(message.getQueryValues().toByteArray())));
		}
		else {
			super.onIncomingControlMessage(message);
		}
	}

	private void startQuery(Q query) {
		if (activeQueries.containsKey(query.QueryId))
			throw new RuntimeException("Thready with this ID already active: " + query.QueryId);
		querySuperstepNos.put(query.QueryId, 0);
		queryChannelBarrierWaitSet.put(query.QueryId, new HashSet<>());

		for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
			vertex.startQuery(query.QueryId);
		}

		Map<Integer, List<M>> queryMessageSlots = new HashMap<>();
		for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
			localVerticesIdMap.put(vertex.ID, vertex);
			queryMessageSlots.put(vertex.ID, new ArrayList<>());
		}
		synchronized (queryInVertexMessages) {
			queryInVertexMessages.put(query.QueryId, queryMessageSlots);
		}

		activeQueries.put(query.QueryId, query);
		activeQueriesLocal.put(query.QueryId, globalValueFactory.createClone(query));

		logger.info("Worker started query " + query.QueryId);
	}

	@Override
	public void onIncomingVertexMessage(int msgSuperstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages) {
		int superstepNo = querySuperstepNos.get(queryId);
		// Discover vertices if enabled. Only discover for broadcast messages as they are a sign that vertices are unknown.
		if (broadcastFlag && Settings.VERTEX_MACHINE_DISCOVERY) {
			final HashSet<Integer> srcVertices = new HashSet<>();
			for (final Pair<Integer, M> msg : vertexMessages) {
				if (localVerticesIdMap.containsKey(msg.first)) {
					srcVertices.add(msg.first);
				}
			}
			// Also discover all src vertices from this incoming broadcast message, if enabled
			if (Settings.VERTEX_MACHINE_DISCOVERY_INCOMING) {
				for (final Integer srcVertId : srcVertices) {
					remoteVertexMachineRegistry.addEntry(srcVertId, srcMachine);
					//if (remoteVertexMachineRegistry.addEntry(srcVertId, srcMachine)) superstepStats.NewVertexMachinesDiscovered++;
				}
			}
			// Send get-to-know message for all own vertices in this broadcast message
			// Sender did not known these vertices, thats why a broadcast was sent.
			if (!srcVertices.isEmpty()) messaging.sendGetToKnownMessage(srcMachine, srcVertices);
		}

		// Normal messages have content.
		// Vertex messages without content are "get-to-know messages", only for vertex registry

		if (msgSuperstepNo < superstepNo) {
			logger.error("Message from past superstep in superstep " + superstepNo + " from " + srcMachine);
		}
		else {
			// TODO Sort to correct vertex, check superstpNp
			synchronized (queryInVertexMessages) {
				for (final Pair<Integer, M> msg : vertexMessages) {
					final List<M> localMsgQueue = queryInVertexMessages.get(queryId).get(msg.first);
					if (localMsgQueue != null) {
						//superstepStats.ReceivedCorrectVertexMessages++; // TODO Check superstep etc, sort by query
						localMsgQueue.add(msg.second);
					}
					//					else {
					//						superstepStats.ReceivedWrongVertexMessages++;
					//					}
				}
			}
		}
	}

	@Override
	public void onIncomingGetToKnowMessage(int srcMachine, Collection<Integer> srcVertices) {
		for (final Integer srcVertex : srcVertices) {
			remoteVertexMachineRegistry.addEntry(srcVertex, srcMachine);
			//if (remoteVertexMachineRegistry.addEntry(srcVertex, srcMachine))
			//superstepStats.NewVertexMachinesDiscovered++;
		}
	}

	@Override
	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}
}
