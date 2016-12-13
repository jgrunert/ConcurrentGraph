package mthesis.concurrent_graph.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.AbstractVertex;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.Messages.VertexMessage;
import mthesis.concurrent_graph.communication.VertexMessageBuildUtil;
import mthesis.concurrent_graph.util.Pair;

/**
 * Concurrent graph processing worker main
 */
public class WorkerMachine extends AbstractMachine {
	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String output;

	private final Class<? extends AbstractVertex> vertexClass;
	private final List<AbstractVertex> vertices;
	private final Set<Integer> vertexIds = new HashSet<>();
	private final Set<Integer> channelBarrierWaitSet = new HashSet<>();
	private final Map<Integer, List<VertexMessage>> vertexMessageBuckets = new HashMap<>();
	private final List<VertexMessage> bufferedLoopbackMessages = new ArrayList<>();
	protected final List<VertexMessage> inVertexMessages = new ArrayList<>();

	private int superstepNo;
	private SuperstepStats superstepStats;

	private final Set<Integer> localVertices = new HashSet<>();
	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();


	public WorkerMachine(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> workerIds, int masterId,
			String output, Class<? extends AbstractVertex> vertexClass) {
		super(machines, ownId);
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		this.masterId = masterId;

		this.vertices = new ArrayList<>();
		this.output = output;
		this.vertexClass = vertexClass;
	}

	private void loadVertices(String input) {
		try (BufferedReader br = new BufferedReader(new FileReader(input))) {
			String line;
			final List<Integer> edges = new ArrayList<>();

			int currentVertex;
			if((line = br.readLine()) != null)
				currentVertex = Integer.parseInt(line);
			else
				return;

			while ((line = br.readLine()) != null) {
				if (line.startsWith("\t")) {
					edges.add(Integer.parseInt(line.substring(1)));
				} else {
					addVertex(currentVertex, edges);
					edges.clear();
					currentVertex = Integer.parseInt(line);
				}
			}
			addVertex(currentVertex, edges);
		} catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}

		for(final Integer vertexId : vertexIds) {
			localVertices.add(vertexId);
			vertexMessageBuckets.put(vertexId, new ArrayList<>());
		}
	}
	private void addVertex(int vertexId, List<Integer> edges) {
		Constructor<?> c;
		try {
			c = vertexClass.getDeclaredConstructor(List.class, int.class, WorkerMachine.class);
			c.setAccessible(true);
			vertices.add((AbstractVertex)c.newInstance(new ArrayList<>(edges), vertexId, this));
			vertexIds.add(vertexId);
		}
		catch (final Exception e) {
			logger.error("Creating vertex " + vertexId + " failed", e);
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
		for(final String partition : assignedPartitions) {
			loadVertices(partition);
		}
		superstepStats.ActiveVertices = vertices.size();
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
				logger.debug("Starting superstep " + superstepNo); // TODO trace


				// Compute and Messaging (done by vertices)
				for(final AbstractVertex vertex : vertices) {
					final List<VertexMessage> vertMsgs = vertexMessageBuckets.get(vertex.id);
					vertex.superstep(vertMsgs, superstepNo);
					vertMsgs.clear();
					if(vertex.isActive())
						superstepStats.ActiveVertices++;
				}
				logger.debug("Worker finished superstep compute " + superstepNo + " activeVertices: " + superstepStats.ActiveVertices);


				// Barrier sync with other workers;
				sendWorkersSuperstepFinished();
				waitForWorkerSuperstepsFinished();
				logger.debug("Worker finished superstep barrier " + superstepNo);


				// Sort messages from buffers after barrier sync
				// Incoming messages
				synchronized (inVertexMessages) {
					for(final VertexMessage msg : inVertexMessages) {
						if(msg.getSuperstepNo() != superstepNo) {
							logger.error("Message from wrong superstep: " + msg);
							continue;
						}
						final List<VertexMessage> vertMsgs = vertexMessageBuckets.get(msg.getDstVertex());
						if(vertMsgs != null) {
							superstepStats.ReceivedCorrectVertexMessages++;
							vertMsgs.add(msg);
						}
						else {
							//System.out.println(ownId + " WRONG " + msg.getSrcVertex() + " " + msg.getDstVertex() + " " + msg.getContent());
							//System.out.println(ownId + " WRONG " + msg.getDstVertex() + " from " + msg.getSrcMachine());
							superstepStats.ReceivedWrongVertexMessages++;
							if(!msg.hasBroadcastFlat())
								logger.warn("Recieved wrong vertex unicast message: " + msg);
						}
					}
					inVertexMessages.clear();
				}
				// Loopback messages
				for(final VertexMessage msg : bufferedLoopbackMessages) {
					if(msg.getSuperstepNo() != superstepNo) {
						logger.error("Message from wrong superstep: " + msg);
						continue;
					}
					final List<VertexMessage> vertMsgs = vertexMessageBuckets.get(msg.getDstVertex());
					if(vertMsgs != null)
						vertMsgs.add(msg);
				}
				bufferedLoopbackMessages.clear();
				logger.debug("Worker finished superstep message sort " + superstepNo);


				// Signal master that ready
				superstepStats.TotalVertexMachinesDiscovered = remoteVertexMachineRegistry.getRegistrySize();
				sendMasterSuperstepFinished();
			}
		}
		finally {
			logger.info("Worker finishing");
			writeOutput();
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
		messaging.sendMessageBroadcast(otherWorkerIds, ControlMessageBuildUtil.Build_Worker_Superstep_Barrier(superstepNo, ownId), true);
	}

	private void sendMasterSuperstepFinished() {
		superstepStats.SentControlMessages++;
		messaging.sendMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Superstep_Finished(superstepNo, ownId,
				superstepStats), true);
	}

	private void sendMasterFinishedMessage() {
		superstepStats.SentControlMessages++;
		messaging.sendMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Finished(superstepNo, ownId), true);
	}

	/**
	 * Sends a vertex message. If local vertex, direct loopback.
	 * It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	public void sendVertexMessage(int srcVertex, int dstVertex, Integer content) {

		if(localVertices.contains(dstVertex)) {
			// Local message
			final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content, false);
			//System.out.println(ownId + " SEND LOCAL " + srcVertex + " " + dstVertex);
			superstepStats.SentVertexMessagesLocal++;
			bufferedLoopbackMessages.add(message.getVertexMessage());
		}
		else {
			// Remote message
			final Integer remoteMachine = remoteVertexMachineRegistry.lookupEntry(dstVertex);
			if(remoteMachine != null) {
				// Unicast remote message
				final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content, false);
				//System.out.println(ownId + " SEND UNI from " + srcVertex + " to " + dstVertex + ":" + remoteMachine);
				superstepStats.SentVertexMessagesUnicast++;
				messaging.sendMessageUnicast(remoteMachine, message, false);
			}
			else {
				// Broadcast remote message
				final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content, true);
				//System.out.println(ownId + " SEND BCAST " + srcVertex + " to " + dstVertex + " " + otherWorkerIds);
				superstepStats.SentVertexMessagesBroadcast += otherWorkerIds.size();
				messaging.sendMessageBroadcast(otherWorkerIds, message, false);
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int srcVertex, int dstVertex, int dstMachine, Integer content) {
		//System.out.println(ownId + " SEND DIR " + srcVertex + " " + dstVertex + " to " + dstMachine);
		superstepStats.SentVertexMessagesUnicast++;
		final MessageEnvelope message = createVertexMessageEnvelope(srcVertex, dstVertex, content, false);
		messaging.sendMessageUnicast(dstMachine, message, false);
	}

	private MessageEnvelope createVertexMessageEnvelope(int srcVertex, int dstVertex, Integer content, boolean broadcastFlag) {
		if(content != null)
			return VertexMessageBuildUtil.BuildWithContent(superstepNo, ownId, srcVertex, dstVertex, broadcastFlag, content);
		else
			return VertexMessageBuildUtil.BuildWithoutContent(superstepNo, ownId, srcVertex, dstVertex, broadcastFlag);
	}

	@Override
	public void onIncomingVertexMessage(VertexMessage message) {
		//final boolean registered = remoteVertexMachineRegistry.lookupEntry(message.getSrcVertex()) != null;
		// Update vertex registry if discovery enabled
		if(Settings.VERTEX_DISCOVERY) {
			if(remoteVertexMachineRegistry.addEntry(message.getSrcVertex(), message.getSrcMachine())) {
				//System.out.println(ownId + " LEARNED " + message.getSrcVertex() + ":" + message.getSrcMachine());
				superstepStats.NewVertexMachinesDiscovered++;
				if(Settings.ACTIVE_VERTEX_DISCOVERY && message.hasContent() && localVertices.contains(message.getDstVertex())) {
					// Send "get-to-know message" without content. Dont reply on received messages without content.
					//System.out.println(ownId + " gtkm to " + message.getSrcMachine());
					sendVertexMessageToMachine(message.getDstVertex(), -1, message.getSrcMachine(), null);
				}
			}
		}

		// Vertex messages without content are "get-to-know messages", only for vertex registry
		if(message.hasContent()) {
			synchronized (inVertexMessages) {
				//System.out.println(ownId + " REC " + message.getSrcVertex() + " " + message.getDstVertex() + " " + message.getContent());
				inVertexMessages.add(message);
			}
		}
		//		else {
		//			final boolean registered2 = remoteVertexMachineRegistry.lookupEntry(message.getSrcVertex()) != null;
		//			System.out.println("GetToKnow " + registered + " " + registered2);
		//		}
	}


	private void writeOutput() {
		try(PrintWriter writer = new PrintWriter(new FileWriter(output + File.separator + ownId + ".txt")))
		{
			for(final AbstractVertex vertex : vertices) {
				writer.println(vertex.id + "\t" + vertex.getOutput());
			}
		}
		catch(final Exception e)
		{
			logger.error("writeOutput failed", e);
		}
	}
}
