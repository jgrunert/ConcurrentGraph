package mthesis.concurrent_graph.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.ControlMessage;
import mthesis.concurrent_graph.communication.MessageType;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.node.AbstractNode;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.CCDetectVertex;

/**
 * Concurrent graph processing worker main
 */
public class WorkerNode extends AbstractNode {	
	//private List<Integer> workers;
	private final int otherWorkerCount;
	
	private final List<CCDetectVertex> vertices;
	
	private int superstepNo;
	private Map<Integer, List<VertexMessage>> vertexMessageBuckets = new HashMap<>();

	
	public WorkerNode(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> allWorkers, 
			Set<Integer> vertexIds, String dataDir) {
		super(machines, ownId);
		//this.workers = workers;
		otherWorkerCount = allWorkers.size() - 1;
		
		this.vertices = new ArrayList<>(vertexIds.size());
		loadVertices(vertexIds, dataDir);
	}
	
	private void loadVertices(Set<Integer> vertexIds, String dataDir) {
		try (BufferedReader br = new BufferedReader(new FileReader(dataDir))) {
			String line;
			List<Integer> edges = new ArrayList<>();

			int currentVertex;
			if((line = br.readLine()) != null)
				currentVertex = Integer.parseInt(line);
			else
				return;
			
			while ((line = br.readLine()) != null) {
				if (line.startsWith("\t")) {
					edges.add(Integer.parseInt(line.substring(1)));
				} else {
					if(vertexIds.contains(currentVertex))
						addVertex(currentVertex, edges);
					currentVertex = Integer.parseInt(line);
				}
			}
		} catch (Exception e) {
			logger.error("loadVertices failed", e);
		}
		
		for(Integer vertexId : vertexIds) {
			vertexMessageBuckets.put(vertexId, new ArrayList<>());
		}
	}	
	private void addVertex(int vertexId, List<Integer> edges) {
		vertices.add(new CCDetectVertex(edges, vertexId, this));
	}
	

	@Override
	public void run() {
		logger.info("Waiting for started worker node " + ownId);
		waitUntilStarted();
		
		logger.info("Starting run worker node " + ownId);
		
		broadcastControlMessage(MessageType.Control_Node_Superstep_Finished, -1, "Ready");
		if (!waitForNextSuperstep()) {
			logger.error("Failed to wait for superstep");
			return;
		}
		superstepNo = 0;
		
		while(true) {			
			logger.debug("Starting superstep " + superstepNo); // TODO trace?
			
			// Sort incoming messages		
			for(VertexMessage msg : inWorkerMessages) {
				List<VertexMessage> vertMsgs = vertexMessageBuckets.get(msg.To);
				if(vertMsgs != null)
					vertMsgs.add(msg);
			}
			inWorkerMessages.clear();
			
			// Compute and Messaging (done by vertices)
			for(AbstractVertex vertex : vertices) {
				List<VertexMessage> vertMsgs = vertexMessageBuckets.get(vertex.id);
				vertex.compute(vertMsgs);
				vertMsgs.clear();
			}

			// TODO Evaluate if all nodes inactive
			
			// Barrier sync
			broadcastControlMessage(MessageType.Control_Node_Superstep_Finished, superstepNo, "Ready");
			if (!waitForNextSuperstep()) {
				logger.error("Failed to wait for superstep");
				return;
			}
			superstepNo++;
		}
	}
	
	
	public boolean waitForNextSuperstep() {
		boolean success = waitForControlMessages(MessageType.Control_Node_Superstep_Finished);		
		synchronized (inControlMessages) {
			List<ControlMessage> messages = inControlMessages.get(MessageType.Control_Node_Superstep_Finished);
			if(messages != null)
				messages.clear();
		}
		return success;
	}
	
	public boolean waitForControlMessages(MessageType type) {
		while(!Thread.interrupted()) {
			synchronized (inControlMessages) {
				List<ControlMessage> messages = inControlMessages.get(type);
				if(messages != null && messages.size() >= otherWorkerCount)
					return true;
			}
			Thread.yield(); // TODO
		}
		return false;
	}
}
