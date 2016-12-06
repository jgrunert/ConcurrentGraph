package mthesis.concurrent_graph.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.ControlMessage;
import mthesis.concurrent_graph.communication.MessageType;
import mthesis.concurrent_graph.node.AbstractNode;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.CCDetectVertex;

/**
 * Concurrent graph processing worker main
 */
public class WorkerNode extends AbstractNode {
	private final Logger logger;
	
	//private List<Integer> workers;
	private final int otherWorkerCount;
	
	private final List<CCDetectVertex> vertices;

	
	public WorkerNode(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> allWorkers, 
			Set<Integer> vertexIds, String dataDir) {
		super(machines, ownId);
		this.logger = LoggerFactory.getLogger(this.getClass() + "[" + ownId + "]");
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
	}	
	private void addVertex(int vertexId, List<Integer> edges) {
		vertices.add(new CCDetectVertex(edges, vertexId, this));
	}
	

	@Override
	public void run() {
		broadcastControlMessage(MessageType.Control_Node_Finished, 0, "Ready");
	}
	
	
	public void waitForControlMessages(MessageType type) {
		while(true) {
			synchronized (inControlMessages) {
				List<ControlMessage> messages = inControlMessages.get(type);
				if(messages != null && messages.size() >= otherWorkerCount)
					break;
			}
			Thread.yield(); // TODO
		}
		
		synchronized (inControlMessages) {
			inControlMessages.get(type).clear();
		}
	}
}
