package mthesis.concurrent_graph.vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import mthesis.concurrent_graph.worker.GlobalObjects;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;


public abstract class AbstractVertex<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> {
	public final int ID;
	private V value;

	public List<VertexMessage<M>> messagesNextSuperstep = new ArrayList<>();
	private List<Edge<E>> edges;

	protected int superstepNo = 0;
	private final VertexWorkerInterface<M> worker;
	private boolean votedHalt = false;


	public AbstractVertex(int id, VertexWorkerInterface<M> worker) {
		super();
		this.ID = id;
		this.setEdges(edges);
		this.worker = worker;
	}


	public void superstep(int superstep) {
		if(isActive()) {
			this.superstepNo = superstep;
			compute(messagesNextSuperstep);
			messagesNextSuperstep.clear();
		}
	}

	protected abstract void compute(List<VertexMessage<M>> messages);


	protected void sendMessageToAllOutgoingEdges(M message) {
		for (final Edge<E> edge : edges) {
			worker.sendVertexMessage(ID, edge.TargetVertexId, message);
		}
	}

	protected void sendMessageToVertex(M message, int sendTo) {
		worker.sendVertexMessage(ID, sendTo, message);
	}

	protected void sendMessageToVertices(M message, Collection<Integer> sendTo) {
		for (final Integer st : sendTo) {
			worker.sendVertexMessage(ID, st, message);
		}
	}


	protected void voteHalt() {
		votedHalt = true;
	}

	public boolean isActive() {
		return !(votedHalt && messagesNextSuperstep.isEmpty());
	}


	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}


	public List<Edge<E>> getEdges() {
		return edges;
	}


	public void setEdges(List<Edge<E>> edges) {
		this.edges = edges;
	}


	public GlobalObjects getGlobalObjects() {
		return worker.getGlobalObjects();
	}


	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "_" + ID + "(" + valueToString() + ")," + edges;
	}

	private String valueToString() {
		if(value == null)
			return "";
		return value.getString();
	}
}
