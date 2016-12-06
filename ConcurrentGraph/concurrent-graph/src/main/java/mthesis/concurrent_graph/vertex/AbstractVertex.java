package mthesis.concurrent_graph.vertex;

import java.util.List;

import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.worker.WorkerNode;


public abstract class AbstractVertex {
	protected final List<Integer> neighbors;
	protected final int id;
	protected int superstepNo = 0;
	private final WorkerNode workerManager;
	private boolean active = true;
	

	public AbstractVertex(List<Integer> neighbors, int id, WorkerNode workerManager) {
		super();
		this.id = id;
		this.neighbors = neighbors;
		this.workerManager = workerManager;
	}

	
	public void superstep(List<VertexMessage> messages, int superstep) {
		if (!messages.isEmpty())
			active = true;
		this.superstepNo = superstep;
		compute(messages);
	}

	public abstract void compute(List<VertexMessage> messages);

	protected void sendMessageToAllNeighbors(String message) {
		for (int i = 0; i < neighbors.size(); i++) {
			workerManager.sendVertexMessage(id, neighbors.get(i), superstepNo, message);
		}
	}
	
	protected void voteHalt() {
		active = false;
	}
	
	
	public boolean isActive() {
		return active;
	}
}
