package mthesis.concurrent_graph.worker;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.GlobalStatsMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.Messages.VertexMessageTransport;
import mthesis.concurrent_graph.communication.VertexMessageBuildUtil;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexMessage;
import mthesis.concurrent_graph.writable.BaseWritable;

/**
 * Concurrent graph processing worker main
 */
public class WorkerMachine<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> extends AbstractMachine implements VertexWorkerInterface<M>{
	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String outputDir;

	private final JobConfiguration<V, E, M> jobConfig;
	private final BaseWritable.BaseWritableFactory<M> vertexMessageFactory;

	private List<AbstractVertex<V, E, M>> localVerticesList;
	private final Map<Integer, AbstractVertex<V, E, M>> localVerticesIdMap = new HashMap<>();
	private final GlobalObjects globalObjects = new GlobalObjects();

	private final Set<Integer> channelBarrierWaitSet = new HashSet<>();
	private final List<VertexMessage<M>> bufferedLoopbackMessages = new ArrayList<>();
	protected final List<VertexMessage<M>> inVertexMessages = new ArrayList<>();
	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	private volatile int superstepNo;
	private SuperstepStats superstepStats;



	public WorkerMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, int masterId,
			String outputDir, JobConfiguration<V, E, M> jobConfig) {
		super(machines, ownId);
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		this.masterId = masterId;

		this.outputDir = outputDir;
		this.jobConfig = jobConfig;
		this.vertexMessageFactory = jobConfig.getMessageValueFactory();
	}

	private void loadVertices(List<String> partitions) {
		localVerticesList = new VertexTextInputReader<V, E, M>().getVertices(partitions, jobConfig, this);

		for(final AbstractVertex<V, E, M> vertex : localVerticesList) {
			if(vertex == null)
				throw new RuntimeException("aaaaaa");
			localVerticesIdMap.put(vertex.ID, vertex);
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
		superstepStats.ActiveVertices = localVerticesList.size();
		sendMasterSuperstepFinished();

		try {
			while(!Thread.interrupted()) {
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
				logger.info("Worker starting superstep compute " + superstepNo);
				for(final AbstractVertex<V, E, M> vertex : localVerticesList) {
					//final List<VertexMessage<M>> vertMsgs = vertexMessageBuckets.get(vertex.ID);
					vertex.superstep(superstepNo);
					//vertMsgs.clear();
				}
				logger.debug("Worker finished superstep compute " + superstepNo);


				// Barrier sync with other workers;
				sendWorkersSuperstepFinished();
				waitForWorkerSuperstepsFinished();
				logger.debug("Worker finished superstep barrier " + superstepNo);


				// Sort messages from buffers after barrier sync
				// Incoming messages
				synchronized (inVertexMessages) {
					for(final VertexMessage<M> msg : inVertexMessages) {
						if(msg.SuperstepNo != superstepNo){
							logger.error("Remote vertex message from wrong superstep: " + msg.SuperstepNo);
							continue;
						}
						final AbstractVertex<V, E, M> vertex = localVerticesIdMap.get(msg.DstVertex);
						if(vertex != null) {
							superstepStats.ReceivedCorrectVertexMessages++;
							vertex.messagesNextSuperstep.add(msg);
						}
						else {
							superstepStats.ReceivedWrongVertexMessages++;
						}
					}
					inVertexMessages.clear();
				}
				// Loopback messages
				for(final VertexMessage<M> msg : bufferedLoopbackMessages) {
					if(msg.SuperstepNo != superstepNo){
						logger.error("Local vertex message from wrong superstep: " + msg.SuperstepNo);
						continue;
					}
					final AbstractVertex<V, E, M> vertex = localVerticesIdMap.get(msg.DstVertex);
					if(vertex != null)
						vertex.messagesNextSuperstep.add(msg);
					else
						logger.warn("Local vertex message for unknown vertex");

				}
				bufferedLoopbackMessages.clear();

				// Count active vertices
				for(final AbstractVertex<V, E, M> vertex : localVerticesList) {
					if(vertex.isActive())
						superstepStats.ActiveVertices++;
				}
				logger.debug("Worker finished superstep message sort " + superstepNo + " activeVertices: " + superstepStats.ActiveVertices);

				// Signal master that ready
				superstepStats.TotalVertexMachinesDiscovered = remoteVertexMachineRegistry.getRegistrySize();
				sendMasterSuperstepFinished();
			}
		}
		finally {
			logger.info("Worker finishing");
			new VertexTextOutputWriter<V, E, M>().writeOutput(outputDir + File.separator + ownId + ".txt", localVerticesList);
			sendMasterFinishedMessage();
			stop();
		}
	}


	public boolean waitForWorkerSuperstepsFinished() {
		try {
			while(!Thread.interrupted() && !channelBarrierWaitSet.isEmpty()) {
				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);
				if(msg != null) {
					switch (msg.getType()) {
						case Worker_Superstep_Barrier:
							if(msg.getSuperstepNo() == superstepNo) {
								channelBarrierWaitSet.remove(msg.getSrcMachine());
							} else {
								logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: "
										+ msg.getSuperstepNo() + " at step " + superstepNo);
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

			if(msg != null) {
				if(msg.getType() == ControlMessageType.Master_Next_Superstep) {
					return msg.getAssignPartitions().getPartitionFilesList();
				} else {
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
			while(!Thread.interrupted()) {
				final ControlMessage msg = inControlMessages.poll(Settings.MESSAGE_TIMEOUT, TimeUnit.MILLISECONDS);

				if(msg != null) {
					switch (msg.getType()) {
						case Master_Next_Superstep:
							if(msg.getSuperstepNo() == superstepNo + 1) {
								final GlobalStatsMessage globalStatsMsg = msg.getGlobalStats();
								globalObjects.setVertexCount(globalStatsMsg.getVertexCount());
								globalObjects.setActiveVertices(globalStatsMsg.getActiveVertices());
								return true;
							} else {
								logger.error("Received Master_Next_Superstep with wrong superstepNo: "
										+ msg.getSuperstepNo() + " at step " + superstepNo);
								return false;
							}
						case Master_Finish:
							logger.info("Received Master_Finish");
							return false;
						case Worker_Superstep_Barrier:  // Barrier from workers which are finished before we even started
							if(msg.getSuperstepNo() == superstepNo + 1) {
								channelBarrierWaitSet.remove(msg.getSrcMachine());
							} else {
								logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: "
										+ msg.getSuperstepNo() + " at wait for " + (superstepNo + 1));
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
		messaging.sendControlMessageBroadcast(otherWorkerIds, ControlMessageBuildUtil.Build_Worker_Superstep_Barrier(superstepNo, ownId), true);
	}

	private void sendMasterSuperstepFinished() {
		superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Superstep_Finished(superstepNo, ownId,
				superstepStats, localVerticesList.size()), true);
	}

	private void sendMasterFinishedMessage() {
		superstepStats.SentControlMessages++;
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Finished(superstepNo, ownId), true);
	}

	/**
	 * Sends a vertex message. If local vertex, direct loopback.
	 * It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	@Override
	public void sendVertexMessage(int srcVertex, int dstVertex, M content) {

		if(localVerticesIdMap.containsKey(dstVertex)) {
			// Local message
			//System.out.println(ownId + " SEND LOCAL " + srcVertex + " " + dstVertex);
			superstepStats.SentVertexMessagesLocal++;
			bufferedLoopbackMessages.add(new VertexMessage<M>(superstepNo, srcVertex, dstVertex, content));
		}
		else {
			// Remote message
			final Integer remoteMachine = remoteVertexMachineRegistry.lookupEntry(dstVertex);
			if(remoteMachine != null) {
				// Unicast remote message
				final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content);
				//System.out.println(ownId + " SEND UNI from " + srcVertex + " to " + dstVertex + ":" + remoteMachine);
				superstepStats.SentVertexMessagesUnicast++;
				messaging.sendVertexMessageUnicast(remoteMachine, message, false);
			}
			else {
				// Broadcast remote message
				final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content);
				//System.out.println(ownId + " SEND BCAST " + srcVertex + " to " + dstVertex + " " + otherWorkerIds);
				superstepStats.SentVertexMessagesBroadcast += otherWorkerIds.size();
				messaging.sendVertexMessageBroadcast(otherWorkerIds, message, false);
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int srcVertex, int dstVertex, int dstMachine, M content) {
		//System.out.println(ownId + " SEND DIR " + srcVertex + " " + dstVertex + " to " + dstMachine);
		superstepStats.SentVertexMessagesUnicast++;
		final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content);
		messaging.sendVertexMessageUnicast(dstMachine, message, false);
	}

	private MessageEnvelope createVertexMessageEnvelope(int srcVertex, int dstVertex, M content) {
		if(content != null)
			return VertexMessageBuildUtil.BuildWithContent(superstepNo, ownId, srcVertex, dstVertex, content);
		else
			return VertexMessageBuildUtil.BuildWithoutContent(superstepNo, ownId, srcVertex, dstVertex);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void onIncomingVertexMessage(VertexMessageTransport message) {
		// final boolean registered = remoteVertexMachineRegistry.lookupEntry(message.getSrcVertex()) != null;
		// Update vertex registry if discovery enabled
		if(Settings.VERTEX_DISCOVERY) {
			if(remoteVertexMachineRegistry.addEntry(message.getSrcVertex(), message.getSrcMachine())) {
				superstepStats.NewVertexMachinesDiscovered++;
				if(Settings.ACTIVE_VERTEX_DISCOVERY && message.hasContent() && localVerticesIdMap.containsKey(message.getDstVertex())) {
					sendVertexMessageToMachine(message.getDstVertex(), -1, message.getSrcMachine(), null);
				}
			}
		}

		// Normal messages have content.
		// Vertex messages without content are "get-to-know messages", only for vertex registry
		if(message.hasContent()) {
			if(message.getSuperstepNo() < superstepNo) {
				logger.error("Message from part superstep in superstep " + superstepNo + "\n" + message);
			}
			else {
				final VertexMessage vMsg = new VertexMessage(message.getSuperstepNo(), message.getSrcVertex(), message.getDstVertex(),
						vertexMessageFactory.createFromBytes(message.getContent().asReadOnlyByteBuffer()));
				synchronized (inVertexMessages) {
					inVertexMessages.add(vMsg);
				}
			}
		}
	}

	@Override
	public GlobalObjects getGlobalObjects() {
		return globalObjects;
	}
}
