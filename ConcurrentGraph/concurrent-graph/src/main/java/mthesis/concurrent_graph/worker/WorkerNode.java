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
import java.util.stream.Collectors;

import mthesis.concurrent_graph.communication.ControlMessage;
import mthesis.concurrent_graph.communication.MessageType;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.node.AbstractNode;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;

/**
 * Concurrent graph processing worker main
 */
public class WorkerNode extends AbstractNode {
	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String input;
	private final String output;

	private final Class<? extends AbstractVertex> vertexClass;
	private final List<AbstractVertex> vertices;
	private final Set<Integer> vertexIds = new HashSet<>();
	private int superstepNo;
	private int superstepMessagesSent;
	private final Set<Integer> channelBarrierWaitSet = new HashSet<>();
	private final Map<Integer, List<VertexMessage>> vertexMessageBuckets = new HashMap<>();
	private final List<VertexMessage> bufferedLoopbackMessages = new ArrayList<>();


	public WorkerNode(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> workerIds, int masterId,
			String input, String output, Class<? extends AbstractVertex> vertexClass) {
		super(machines, ownId);
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		this.masterId = masterId;

		this.vertices = new ArrayList<>();
		this.input = input;
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
			vertexMessageBuckets.put(vertexId, new ArrayList<>());
		}
	}
	private void addVertex(int vertexId, List<Integer> edges) {
		Constructor<?> c;
		try {
			c = vertexClass.getDeclaredConstructor(List.class, int.class, WorkerNode.class);
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

		// Wait for master to signal that input ready
		superstepNo = -2;
		if (!waitForNextSuperstep(false)) {
			logger.error("Wait for input ready failed");
			return;
		}
		superstepNo++;
		loadVertices(input);
		sendSuperstepFinishedMessages(vertices.size());

		try {
			while(!Thread.interrupted()) {
				if (!waitForNextSuperstep(true)) {
					break;
				}
				superstepNo++;
				superstepMessagesSent = 0;

				logger.debug("Starting superstep " + superstepNo); // TODO trace

				// Sort incoming messages
				for(final VertexMessage msg : inVertexMessages) {
					if(msg.SuperstepNo != superstepNo - 1) {
						logger.error("Message from wrong superstep: " + msg);
						continue;
					}
					final List<VertexMessage> vertMsgs = vertexMessageBuckets.get(msg.ToVertex);
					if(vertMsgs != null)
						vertMsgs.add(msg);
				}
				inVertexMessages.clear();
				// Sort loopback messages
				for(final VertexMessage msg : bufferedLoopbackMessages) {
					if(msg.SuperstepNo != superstepNo - 1) {
						logger.error("Message from wrong superstep: " + msg);
						continue;
					}
					final List<VertexMessage> vertMsgs = vertexMessageBuckets.get(msg.ToVertex);
					if(vertMsgs != null)
						vertMsgs.add(msg);
				}
				bufferedLoopbackMessages.clear();

				// Compute and Messaging (done by vertices)
				int activeVertices = 0;
				for(final AbstractVertex vertex : vertices) {
					final List<VertexMessage> vertMsgs = vertexMessageBuckets.get(vertex.id);
					vertex.superstep(vertMsgs, superstepNo);
					vertMsgs.clear();
					if(vertex.isActive())
						activeVertices++;
				}

				// Barrier sync
				logger.debug("Worker finished superstep " + superstepNo + " activeVertices: " + activeVertices);
				sendSuperstepFinishedMessages(activeVertices);
			}
		}
		finally {
			logger.info("Worker finishing");
			writeOutput();
			sendFinishedMessage();
			stop();
		}
	}


	public boolean waitForNextSuperstep(boolean waitForWorkers) {
		try {
			boolean masterSignaledNext = false;
			if(waitForWorkers)
				channelBarrierWaitSet.addAll(otherWorkerIds);

			while(!Thread.interrupted() && !(masterSignaledNext && channelBarrierWaitSet.isEmpty())){
				final ControlMessage msg = inControlMessages.take();
				if(msg != null) {
					switch (msg.Type) {
						case Control_Master_Next_Superstep:
							if(msg.SuperstepNo == superstepNo + 1) {
								masterSignaledNext = true;
							} else {
								logger.error("Received Control_Master_Next_Superstep with wrong superstepNo: "
										+ msg.SuperstepNo + " at step " + superstepNo);
							}
							break;
						case Control_Worker_Superstep_Channel_Barrier:
							if(msg.SuperstepNo == superstepNo) {
								channelBarrierWaitSet.remove(msg.FromNode);
							} else {
								logger.error("Received Control_Worker_Superstep_Channel_Barrier with wrong superstepNo: "
										+ msg.SuperstepNo + " at step " + superstepNo);
							}
							break;
						case Control_Master_Finish:
							logger.info("Received Control_Master_Finish");
							return false;

						default:
							logger.error("Illegal control while waitForNextSuperstep: " + msg.Type);
							break;
					}
				}
			}
			return masterSignaledNext && channelBarrierWaitSet.isEmpty();
		}
		catch (final InterruptedException e) {
			return false;
		}
	}


	private void sendSuperstepFinishedMessages(int activeVertices) {
		messaging.sendMessageTo(masterId, MessageType.Control_Worker_Superstep_Finished + ";" + ownId + ";" + superstepNo + ";" + activeVertices + "," +
				superstepMessagesSent);
		messaging.sendMessageTo(otherWorkerIds, MessageType.Control_Worker_Superstep_Channel_Barrier + ";" + ownId + ";" + superstepNo + ";barrier");
	}

	private void sendFinishedMessage() {
		messaging.sendMessageTo(masterId, MessageType.Control_Worker_Finished + ";" + ownId + ";" + superstepNo + ";" + "terminating now");
	}

	public void sendVertexMessage(int fromVertex, int toVertex, String content) {
		superstepMessagesSent++;
		messaging.sendMessageTo(otherWorkerIds, MessageType.Vertex + ";" + ownId + ";" + superstepNo + ";" + fromVertex + ";" + toVertex + ";" + content);
		bufferedLoopbackMessages.add(new VertexMessage(ownId, fromVertex, toVertex, superstepNo, content));
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
