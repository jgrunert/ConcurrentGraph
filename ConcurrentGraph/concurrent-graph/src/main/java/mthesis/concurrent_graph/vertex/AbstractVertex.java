package mthesis.concurrent_graph.vertex;

import java.util.Collection;
import java.util.List;

import mthesis.concurrent_graph.writable.BaseWritable;


public abstract class AbstractVertex<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> {
	public final int ID;
	private V value;

	private List<Edge<E>> edges;

	protected int superstepNo = 0;
	private final VertexMessageSender<M> messageSender;
	private boolean active = true;


	public AbstractVertex(int id, VertexMessageSender<M> messageSender) {
		super();
		this.ID = id;
		this.setEdges(edges);
		this.messageSender = messageSender;
	}


	public void superstep(List<VertexMessage<M>> messages, int superstep) {
		if (!messages.isEmpty())
			active = true;
		if(active) {
			this.superstepNo = superstep;
			compute(messages);
		}
	}

	protected abstract void compute(List<VertexMessage<M>> messages);


	protected void sendMessageToAllOutgoing(M message) {
		for (final Edge<E> edge : edges) {
			messageSender.sendVertexMessage(ID, edge.NeighborId, message);
		}
	}

	protected void sendMessageToVertex(M message, int sendTo) {
		messageSender.sendVertexMessage(ID, sendTo, message);
	}

	protected void sendMessageToVertices(M message, Collection<Integer> sendTo) {
		for (final Integer st : sendTo) {
			messageSender.sendVertexMessage(ID, st, message);
		}
	}


	protected void voteHalt() {
		active = false;
	}

	public boolean isActive() {
		return active;
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
}
