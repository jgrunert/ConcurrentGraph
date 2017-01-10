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
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.QueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.VertexMessageBucket;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

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
 * @param <G>
 *            Global query values type
 */
public class WorkerMachine<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, G extends QueryGlobalValues>
		extends AbstractMachine<M> implements VertexWorkerInterface<M, G> {

	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String outputDir;

	private final JobConfiguration<V, E, M, G> jobConfig;

	private List<AbstractVertex<V, E, M, G>> localVerticesList;
	private final Map<Integer, AbstractVertex<V, E, M, G>> localVerticesIdMap = new HashMap<>();

	private final BaseQueryGlobalValuesFactory<G> globalValueFactory;
	// Global, aggregated QueryGlobalValues. Aggregated and sent by master
	private G globalQueryValues;
	// Local QueryGlobalValues, are sent to master for aggregation.
	private G localQueryValues;

	private final Set<Integer> channelBarrierWaitSet = new HashSet<>();
	protected final Map<Integer, List<M>> inVertexMessages = new HashMap<>();

	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	// Buckets to collect messages to send
	private final VertexMessageBucket<M> vertexMessageBroadcastBucket = new VertexMessageBucket<>();
	private final Map<Integer, VertexMessageBucket<M>> vertexMessageMachineBuckets = new HashMap<>();
	// Buffer for sending outgoing message buckets
	//	private final int[] outVertexMsgDstIdBuffer = new int[Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES];
	//	private final List<M> outVertexMsgContentBuffer = new ArrayList<>(Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES);

	private volatile int superstepNo;
	private SuperstepStats superstepStats;



	public WorkerMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, int masterId, String outputDir,
			JobConfiguration<V, E, M, G> jobConfig) {
		super(machines, ownId, jobConfig.getMessageValueFactory());
		this.masterId = masterId;
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		for (final Integer workerId : otherWorkerIds) {
			vertexMessageMachineBuckets.put(workerId, new VertexMessageBucket<>());
		}
		globalValueFactory = jobConfig.getGlobalValuesFactory();
		localQueryValues = globalValueFactory.createDefault();

		this.outputDir = outputDir;
		this.jobConfig = jobConfig;
	}

	private void loadVertices(List<String> partitions) {
		localVerticesList = new VertexTextInputReader<V, E, M, G>().getVertices(partitions, jobConfig, this);

		synchronized (inVertexMessages) {
			for (final AbstractVertex<V, E, M, G> vertex : localVerticesList) {
				localVerticesIdMap.put(vertex.ID, vertex);
				inVertexMessages.put(vertex.ID, new ArrayList<>());
			}
		}
	}



	@Override
	public void run() {
		logger.info("Starting run worker node " + ownId);
		superstepStats = new SuperstepStats();

		// Wait for master to signal that input ready
		superstepNo = -2;
		final List<String> assignedPartitions = waitForStartup();
		if (assignedPartitions == null) {
			logger.error("Wait for input ready failed");
			return;
		}
		superstepNo++;

		// Load assigned partitions
		loadVertices(assignedPartitions);
		localQueryValues.setVertexCount(localVerticesList.size());
		localQueryValues.setActiveVertices(localVerticesList.size());
		sendMasterSuperstepFinished();

		try {
			while (!Thread.interrupted()) {
				// Wait for start superstep from master
				channelBarrierWaitSet.addAll(otherWorkerIds);
				if (!waitForMasterNextSuperstep()) {
					break;
				}

				// Next superstep
				superstepNo++;
				superstepStats = new SuperstepStats();
				logger.debug("Starting superstep " + superstepNo);


				// Compute and Messaging (done by vertices)
				logger.info("Worker start superstep compute " + superstepNo);
				for (final AbstractVertex<V, E, M, G> vertex : localVerticesList) {
					//final List<VertexMessage<M>> vertMsgs = vertexMessageBuckets.get(vertex.ID);
					vertex.superstep(superstepNo);
					//vertMsgs.clear();
				}
				logger.debug("Worker finished superstep compute " + superstepNo);


				// Barrier sync with other workers;
				flushVertexMessages();
				sendWorkersSuperstepFinished();
				waitForWorkerSuperstepsFinished();
				logger.debug("Worker finished superstep barrier " + superstepNo);


				// Sort messages from buffers after barrier sync
				// Incoming messages
				synchronized (inVertexMessages) {
					for (final Entry<Integer, List<M>> msgQueue : inVertexMessages.entrySet()) {
						final AbstractVertex<V, E, M, G> vertex = localVerticesIdMap.get(msgQueue.getKey());
						if (vertex != null) {
							vertex.messagesNextSuperstep.addAll(msgQueue.getValue());
						}
						else {
							logger.error("Cannot find vertex for messages: " + msgQueue.getKey());
						}
						msgQueue.getValue().clear();
					}
				}

				// Count active vertices
				int activeVertices = 0;
				for (final AbstractVertex<V, E, M, G> vertex : localVerticesList) {
					if (vertex.isActive()) activeVertices++;
				}
				localQueryValues.setActiveVertices(activeVertices);
				logger.debug("Worker finished superstep message sort " + superstepNo + " activeVertices: " + activeVertices);

				// Signal master that ready
				superstepStats.TotalVertexMachinesDiscovered = remoteVertexMachineRegistry.getRegistrySize();
				sendMasterSuperstepFinished();
			}
		}
		catch (final Exception e) {
			logger.error("Exception at worker run", e);
		}
		finally {
			logger.info("Worker finishing");
			new VertexTextOutputWriter<V, E, M, G>().writeOutput(outputDir + File.separator + ownId + ".txt", localVerticesList);
			sendMasterFinishedMessage();
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


	public void flushVertexMessages() {
		sendBroadcastVertexMessageBucket();
		for (final int otherWorkerId : otherWorkerIds) {
			final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(otherWorkerId);
			if (!msgBucket.messages.isEmpty()) sendUnicastVertexMessageBucket(msgBucket, otherWorkerId);
			messaging.flushChannel(otherWorkerId);
		}
	}

	public boolean waitForWorkerSuperstepsFinished() {
		try {
			while (!Thread.interrupted() && !channelBarrierWaitSet.isEmpty()) {
				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
				if (msg != null) {
					switch (msg.getType()) {
						case Worker_Superstep_Barrier:
							if (msg.getSuperstepNo() == superstepNo) {
								channelBarrierWaitSet.remove(msg.getSrcMachine());
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
			return channelBarrierWaitSet.isEmpty();
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
				if (msg.getType() == ControlMessageType.Master_Next_Superstep) {
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

	public boolean waitForMasterNextSuperstep() {
		try {
			while (!Thread.interrupted()) {
				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);

				if (msg != null) {
					switch (msg.getType()) {
						case Master_Next_Superstep:
							if (msg.getSuperstepNo() == superstepNo + 1) {
								globalQueryValues = globalValueFactory
										.createFromBytes(ByteBuffer.wrap(msg.getQueryGlobalValues().toByteArray()));
								return true;
							}
							else {
								logger.error("Received Master_Next_Superstep with wrong superstepNo: " + msg.getSuperstepNo() + " at step "
										+ superstepNo);
								return false;
							}
						case Master_Finish:
							logger.info("Received Master_Finish");
							return false;
						case Worker_Superstep_Barrier: // Barrier from workers which are finished before we even started
							if (msg.getSuperstepNo() == superstepNo + 1) {
								channelBarrierWaitSet.remove(msg.getSrcMachine());
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


	private void sendWorkersSuperstepFinished() {
		superstepStats.SentControlMessages++;
		messaging.sendControlMessageMulticast(otherWorkerIds, ControlMessageBuildUtil.Build_Worker_Superstep_Barrier(superstepNo, ownId),
				true);
	}

	private void sendMasterSuperstepFinished() {
		superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_Superstep_Finished(superstepNo, ownId, superstepStats, localQueryValues), true);
	}

	private void sendMasterFinishedMessage() {
		superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Finished(superstepNo, ownId), true);
	}

	/**
	 * Sends a vertex message. If local vertex, direct loopback. It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	@Override
	public void sendVertexMessage(int dstVertex, M messageContent) {

		final List<M> localMsgQueue = inVertexMessages.get(dstVertex);
		if (localMsgQueue != null) {
			// Local message
			superstepStats.SentVertexMessagesLocal++;
			synchronized (inVertexMessages) {
				localMsgQueue.add(messageContent);
			}
		}
		else {
			// Remote message
			final Integer remoteMachine = remoteVertexMachineRegistry.lookupEntry(dstVertex);
			if (remoteMachine != null) {
				// Unicast remote message
				sendVertexMessageToMachine(remoteMachine, dstVertex, messageContent);
			}
			else {
				// Broadcast remote message
				superstepStats.SentVertexMessagesBroadcast += otherWorkerIds.size();
				vertexMessageBroadcastBucket.addMessage(dstVertex, messageContent);
				if (vertexMessageBroadcastBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
					sendBroadcastVertexMessageBucket();
				}
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int dstMachine, int dstVertex, M messageContent) {
		superstepStats.SentVertexMessagesUnicast++;
		final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(dstMachine);
		msgBucket.addMessage(dstVertex, messageContent);
		if (msgBucket.messages.size() > Settings.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
			sendUnicastVertexMessageBucket(msgBucket, dstMachine);
		}
	}

	private void sendUnicastVertexMessageBucket(VertexMessageBucket<M> msgBucket, int dstMachine) {
		final List<Pair<Integer, M>> msgList = packVertexMessage(msgBucket);
		messaging.sendVertexMessageUnicast(dstMachine, superstepNo, ownId, msgList);
		superstepStats.SentVertexMessagesBuckets++;
	}

	private void sendBroadcastVertexMessageBucket() {
		final List<Pair<Integer, M>> msgList = packVertexMessage(vertexMessageBroadcastBucket);
		messaging.sendVertexMessageBroadcast(otherWorkerIds, superstepNo, ownId, msgList);
		superstepStats.SentVertexMessagesBuckets += otherWorkerIds.size();
	}

	private List<Pair<Integer, M>> packVertexMessage(VertexMessageBucket<M> msgBucket) {
		final List<Pair<Integer, M>> msgList = new ArrayList<>(msgBucket.messages); // TODO Pool instance
		msgBucket.messages.clear();
		return msgList;
	}

	// TODO Flush all channels at end of frame



	@Override
	public void onIncomingVertexMessage(int msgSuperstepNo, int srcMachine, boolean broadcastFlag, List<Pair<Integer, M>> vertexMessages) {
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
					if (remoteVertexMachineRegistry.addEntry(srcVertId, srcMachine)) superstepStats.NewVertexMachinesDiscovered++;
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
			synchronized (inVertexMessages) {
				for (final Pair<Integer, M> msg : vertexMessages) {
					final List<M> localMsgQueue = inVertexMessages.get(msg.first);
					if (localMsgQueue != null) {
						superstepStats.ReceivedCorrectVertexMessages++; // TODO Check superstep etc, sort by query
						localMsgQueue.add(msg.second);
					}
					else {
						superstepStats.ReceivedWrongVertexMessages++;
					}
				}
			}
		}
	}

	@Override
	public void onIncomingGetToKnowMessage(int srcMachine, Collection<Integer> srcVertices) {
		for (final Integer srcVertex : srcVertices) {
			if (remoteVertexMachineRegistry.addEntry(srcVertex, srcMachine)) superstepStats.NewVertexMachinesDiscovered++;
		}
	}

	@Override
	public G getLocalQueryValues() {
		return localQueryValues;
	}

	@Override
	public G getGlobalQueryValues() {
		return globalQueryValues;
	}
}
