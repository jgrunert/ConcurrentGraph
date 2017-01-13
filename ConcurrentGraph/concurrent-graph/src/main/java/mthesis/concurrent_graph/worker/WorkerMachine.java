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
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
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
	private volatile boolean started = false;
	private List<String> assignedPartitions;

	private final JobConfiguration<V, E, M, Q> jobConfig;

	private List<AbstractVertex<V, E, M, Q>> localVerticesList;
	private final Map<Integer, AbstractVertex<V, E, M, Q>> localVerticesIdMap = new HashMap<>();

	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Global, aggregated QueryGlobalValues. Aggregated and sent by master
	private final Map<Integer, WorkerQuery<M, Q>> activeQueries = new HashMap<>();
	// Local QueryGlobalValues, are sent to master for aggregation.
	//	private final Map<Integer, Q> activeQueriesLocal = new HashMap<>();
	//
	//	private final Map<Integer, Set<Integer>> queryChannelBarrierWaitSet = new HashMap<>();
	//	private final Map<Integer, Map<Integer, List<M>>> queryInVertexMessages = new HashMap<>();
	//	private final Map<Integer, Integer> querySuperstepNos = new HashMap<>();

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


		try {
			while (!started) {
				Thread.sleep(100);
			}

			// Initialize, load assigned partitions
			loadVertices(assignedPartitions);
			sendMasterInitialized();

			// Execution loop
			while (!Thread.interrupted()) {
				// Wait for queries
				while (activeQueries.isEmpty()) {
					Thread.sleep(100);
				}

				for (WorkerQuery<M, Q> activeQuery : activeQueries.values()) {
					int queryId = activeQuery.Query.QueryId;
					int superstepNo = activeQuery.SuperstepNo;

					// Next superstep
					activeQuery.ChannelBarrierWaitSet.addAll(otherWorkerIds);
					//superstepStats = new SuperstepStats();
					// Compute and Messaging (done by vertices)
					logger.debug("Worker start query " + queryId + " superstep compute " + superstepNo);
					for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
						//final List<VertexMessage<M>> vertMsgs = vertexMessageBuckets.get(vertex.ID);
						vertex.superstep(superstepNo, activeQuery.Query);
						//vertMsgs.clear();
					}
					logger.debug("Worker finished query " + queryId + " superstep compute " + superstepNo);


					// Barrier sync with other workers;
					flushVertexMessages(queryId);
					sendWorkersSuperstepFinished(activeQuery);
					waitForWorkerSuperstepsFinished(activeQuery);
					logger.debug("Worker finished query " + queryId + " superstep barrier " + superstepNo);


					// Sort messages from buffers after barrier sync
					// Incoming messages
					synchronized (activeQuery.InVertexMessages) {
						for (final Entry<Integer, List<M>> msgQueue : activeQuery.InVertexMessages.entrySet()) {
							final AbstractVertex<V, E, M, Q> vertex = localVerticesIdMap.get(msgQueue.getKey());
							if (vertex != null) {
								if (!msgQueue.getValue().isEmpty())
									vertex.queryMessagesNextSuperstep.get(activeQuery.Query.QueryId).addAll(msgQueue.getValue());
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
						if (vertex.isActive(activeQuery.Query.QueryId)) activeVertices++;
					}
					activeQuery.QueryLocal.setActiveVertices(activeVertices);
					logger.debug("Worker finished query " + queryId + " superstep message sort " + superstepNo + " activeVertices: "
							+ activeVertices);

					// Signal master that ready
					//superstepStats.TotalVertexMachinesDiscovered = remoteVertexMachineRegistry.getRegistrySize();
					sendMasterSuperstepFinished(activeQuery);


					// Wait for start superstep from master
					if (!waitForMasterNextSuperstep(activeQuery, activeQuery.SuperstepNo + 1)) {
						new VertexTextOutputWriter<V, E, M, Q>().writeOutput(
								outputDir + File.separator + activeQuery.Query.QueryId + File.separator + ownId + ".txt", localVerticesList,
								activeQuery.Query.QueryId);
						sendMasterQueryFinishedMessage(activeQuery);
						for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
							vertex.finishQuery(activeQuery.Query.QueryId);
						}
						logger.info("Worker finished query " + activeQuery.Query.QueryId);
						synchronized (activeQuery) {
							activeQueries.remove(queryId);
						}
						//activeQuery = null;
						break;
					}

					activeQuery.SuperstepNo++;
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
		WorkerQuery<M, Q> query = activeQueries.get(queryId);
		sendBroadcastVertexMessageBucket(queryId, query.SuperstepNo);
		for (final int otherWorkerId : otherWorkerIds) {
			final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(otherWorkerId);
			if (!msgBucket.messages.isEmpty()) sendUnicastVertexMessageBucket(msgBucket, otherWorkerId, queryId, query.SuperstepNo);
			messaging.flushChannel(otherWorkerId);
		}
	}

	// TODO Remove
	public boolean waitForWorkerSuperstepsFinished(WorkerQuery<M, Q> workerQuery) {
		try {
			while (!Thread.interrupted() && !workerQuery.ChannelBarrierWaitSet.isEmpty()) {
				Thread.sleep(1);
			}
			return workerQuery.Finished;
		}
		catch (final InterruptedException e) {
			return false;
		}
	}

	// Waits for startup, returns assigned partitions
	//	public List<String> waitForStartup() {
	//		try {
	//			final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
	//
	//			if (msg != null) {
	//				if (msg.getType() == ControlMessageType.Master_Worker_Initialize) {
	//					return msg.getAssignPartitions().getPartitionFilesList();
	//				}
	//				else {
	//					logger.error("Illegal control while waitForStartup: " + msg.getType());
	//					return null;
	//				}
	//			}
	//			else {
	//				logger.error("Timeout while waitForStartup");
	//				return null;
	//			}
	//		}
	//		catch (final InterruptedException e) {
	//			logger.error("Interrupt while waitForStartup");
	//			return null;
	//		}
	//	}

	// TODO Remove this ugly thing
	/**
	 * Waits for master to signal next superstep
	 * @return True if continueing with this query, fals otherwise
	 */
	public boolean waitForMasterNextSuperstep(WorkerQuery<M, Q> query, int nextSuperstep) {
		//		int superstepNo = querySuperstepNos.get(queryId);
		try {
			while (!Thread.interrupted() && query.SuperstepNo < nextSuperstep) {
				Thread.sleep(1);
			}
			return query.Finished;
			//				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
			//
			//				if (msg != null) {
			//					switch (msg.getType()) {
			//						case Master_Query_Next_Superstep:
			//							if (msg.getSuperstepNo() == superstepNo + 1) {
			//								Q query = deserializeQuery(msg.getQueryValues());
			//								activeQueries.put(queryId, query);
			//								activeQueriesLocal.put(queryId, globalValueFactory.createClone(query));
			//								return true;
			//							}
			//							else {
			//								logger.error("Received Master_Next_Superstep with wrong superstepNo: " + msg.getSuperstepNo() + " at step "
			//										+ superstepNo);
			//								return false;
			//							}
			//						case Master_Query_Finished:
			//							logger.info("Received Master_Finish");
			//							return false;
			//						case Worker_Query_Superstep_Barrier: // Barrier from workers which are finished before we even started
			//							if (msg.getSuperstepNo() == superstepNo + 1) {
			//								queryChannelBarrierWaitSet.get(queryId).remove(msg.getSrcMachine());
			//							}
			//							else {
			//								logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: " + msg.getSuperstepNo()
			//										+ " at wait for " + (superstepNo + 1));
			//							}
			//							break;
			//
			//						default:
			//							logger.error("Illegal control while waitForMasterNextSuperstep: " + msg.getType());
			//							return false;
			//					}
			//				}
			//				else {
			//					logger.error("Timeout while waitForMasterNextSuperstep");
			//					return false;
			//				}
			//			}
			//			logger.error("Timeout or interrupt while waitForMasterNextSuperstep");
			//			return false;

		}
		catch (final InterruptedException e) {
			logger.error("Interrupt while waitForMasterNextSuperstep");
			return false;
		}
	}


	private void sendWorkersSuperstepFinished(WorkerQuery<M, Q> workerQuery) {
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageMulticast(otherWorkerIds,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepBarrier(workerQuery.SuperstepNo, ownId, workerQuery.Query), true);
	}

	private void sendMasterInitialized() {
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Initialized(ownId, localVerticesList.size()),
				true);
	}

	private void sendMasterSuperstepFinished(WorkerQuery<M, Q> workerQuery) {
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(workerQuery.SuperstepNo, ownId, workerQuery.QueryLocal), true);
		//ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(superstepNo, ownId, superstepStats, queryLocal), true);
	}

	private void sendMasterQueryFinishedMessage(WorkerQuery<M, Q> workerQuery) {
		//superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QueryFinished(workerQuery.SuperstepNo, ownId, workerQuery.QueryLocal), true);
	}

	/**
	 * Sends a vertex message. If local vertex, direct loopback. It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	@Override
	public void sendVertexMessage(int dstVertex, M messageContent, int queryId) {
		WorkerQuery<M, Q> query = activeQueries.get(queryId);
		Map<Integer, List<M>> inMsgs = query.InVertexMessages;
		final List<M> localVertMsgQueue = inMsgs.get(dstVertex);
		if (localVertMsgQueue != null) {
			// Local message
			//superstepStats.SentVertexMessagesLocal++;
			synchronized (inMsgs) {
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
					sendBroadcastVertexMessageBucket(queryId, query.SuperstepNo);
				}
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int dstMachine, int dstVertex, int queryId, M messageContent) {
		WorkerQuery<M, Q> query = activeQueries.get(queryId);
		//superstepStats.SentVertexMessagesUnicast++;
		final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(dstMachine);
		msgBucket.addMessage(dstVertex, messageContent);
		if (msgBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
			sendUnicastVertexMessageBucket(msgBucket, dstMachine, queryId, query.SuperstepNo);
		}
	}

	private void sendUnicastVertexMessageBucket(VertexMessageBucket<M> msgBucket, int dstMachine, int queryId, int superstepNo) {
		final List<Pair<Integer, M>> msgList = packVertexMessage(msgBucket);
		messaging.sendVertexMessageUnicast(dstMachine, superstepNo, ownId, queryId, msgList);
		//superstepStats.SentVertexMessagesBuckets++;
	}

	private void sendBroadcastVertexMessageBucket(int queryId, int superstepNo) {
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
	public synchronized void onIncomingControlMessage(ControlMessage message) {
		// TODO Everything async
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
					Q query = deserializeQuery(message.getQueryValues());
					WorkerQuery<M, Q> activeQuery = activeQueries.get(query.QueryId);
					if (activeQuery == null) {
						logger.error("Received Master_Next_Superstep for unknown query: " + message);
						return;
					}
					if (message.getSuperstepNo() != activeQuery.SuperstepNo + 1) {
						logger.error("Received Master_Next_Superstep with wrong superstepNo: " + message.getSuperstepNo() + " at step "
								+ activeQuery.SuperstepNo);
						return;
					}
					activeQuery.Query = query;
					activeQuery.QueryLocal = globalValueFactory.createClone(query);
					activeQuery.nextSuperstep();
				}
					break;

				case Master_Query_Finished: {
					Q query = deserializeQuery(message.getQueryValues());
					WorkerQuery<M, Q> activeQuery = activeQueries.get(query.QueryId);
					activeQuery.finish();
				}


				case Worker_Query_Superstep_Barrier: {
					Q query = deserializeQuery(message.getQueryValues());
					WorkerQuery<M, Q> activeQuery = activeQueries.get(query.QueryId);
					if (message.getSuperstepNo() == activeQuery.SuperstepNo) {
						activeQuery.ChannelBarrierWaitSet.remove(message.getSrcMachine());
					}
					else {
						logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: " + message.getSuperstepNo()
								+ " at step " + activeQuery.SuperstepNo);
					}
				}
					break;

				default:
					logger.error("Unknown control message type: " + message);
					break;
			}
		}
	}


	@Override
	public void onIncomingVertexMessage(int msgSuperstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages) {
		WorkerQuery<M, Q> activeQuery = activeQueries.get(queryId);
		int superstepNo = activeQuery.SuperstepNo;
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
			synchronized (activeQuery.InVertexMessages) {
				for (final Pair<Integer, M> msg : vertexMessages) {
					final List<M> localMsgQueue = activeQuery.InVertexMessages.get(msg.first);
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


	private void startQuery(Q query) {
		if (activeQueries.containsKey(query.QueryId))
			throw new RuntimeException("Thready with this ID already active: " + query.QueryId);
		WorkerQuery<M, Q> activeQuery = new WorkerQuery<>(query, globalValueFactory);

		for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
			vertex.startQuery(query.QueryId);
		}

		Map<Integer, List<M>> queryMessageSlots = new HashMap<>();
		for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
			localVerticesIdMap.put(vertex.ID, vertex);
			queryMessageSlots.put(vertex.ID, new ArrayList<>());
		}
		synchronized (activeQuery) {
			activeQueries.put(query.QueryId, activeQuery);
		}

		logger.info("Worker started query " + query.QueryId);
	}


	@Override
	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}

	private Q deserializeQuery(ByteString bytes) {
		return globalValueFactory.createFromBytes(ByteBuffer.wrap(bytes.toByteArray()));
	}
}
