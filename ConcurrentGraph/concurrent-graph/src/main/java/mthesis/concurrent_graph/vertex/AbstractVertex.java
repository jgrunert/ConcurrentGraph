package mthesis.concurrent_graph.vertex;

import java.util.Collection;
import java.util.List;

import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.worker.WorkerNode;


public abstract class AbstractVertex {
	public final int id;
	protected final List<Integer> outgoingNeighbors;
	protected int superstepNo = 0;
	private final WorkerNode workerManager;
	private boolean active = true;


	public AbstractVertex(List<Integer> neighbors, int id, WorkerNode workerManager) {
		super();
		this.id = id;
		this.outgoingNeighbors = neighbors;
		this.workerManager = workerManager;
	}


	public void superstep(List<VertexMessage> messages, int superstep) {
		if (!messages.isEmpty())
			active = true;
		this.superstepNo = superstep;
		compute(messages);
	}

	protected abstract void compute(List<VertexMessage> messages);


	protected void sendMessageToAllOutgoing(int message) {
		for (final Integer nb : outgoingNeighbors) {
			workerManager.sendVertexMessage(id, nb, message);
		}
	}

	protected void sendMessageToVertex(int message, int sendTo) {
		workerManager.sendVertexMessage(id, sendTo, message);
	}

	protected void sendMessageToVertices(int message, Collection<Integer> sendTo) {
		for (final Integer st : sendTo) {
			workerManager.sendVertexMessage(id, st, message);
		}
	}


	protected void voteHalt() {
		active = false;
	}

	public boolean isActive() {
		return active;
	}

	public abstract String getOutput();
}
