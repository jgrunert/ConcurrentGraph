package mthesis.concurrent_graph.vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;


public abstract class AbstractVertex<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public final int ID;

	// TODO More efficient datastructure? Arrays, re-use objects etc
	// value = (V[]) new Object[n + 1];
	private List<Edge<E>> edges;
	public Int2ObjectMap<V> queryValues = new Int2ObjectOpenHashMap<>(Settings.DEFAULT_QUERY_SLOTS);
	private V vertexDefaultValue = null;
	public Int2ObjectMap<List<M>> queryMessagesNextSuperstep = new Int2ObjectOpenHashMap<>(Settings.DEFAULT_QUERY_SLOTS);

	//protected int superstepNo = 0;
	private final VertexWorkerInterface<V, E, M, Q> worker;
	private boolean vertexInactive = false;


	public AbstractVertex(int id, VertexWorkerInterface<V, E, M, Q> worker) {
		super();
		this.ID = id;
		this.worker = worker;
	}

	public void startQuery(int queryId) {
		queryValues.put(queryId, vertexDefaultValue != null ? worker.getVertexValueFactory().createClone(vertexDefaultValue) : null);
		queryMessagesNextSuperstep.put(queryId, new ArrayList<>());
	}

	public void finishQuery(int queryId) {
		queryValues.remove(queryId);
		queryMessagesNextSuperstep.remove(queryId);
	}


	public void superstep(int superstepNo, Q query) {
		//if (isActive()) {
		List<M> messagesNextSuperstep = queryMessagesNextSuperstep.get(query.QueryId);

		if (!(vertexInactive && messagesNextSuperstep.isEmpty())) {
			compute(superstepNo, messagesNextSuperstep, query);
			messagesNextSuperstep.clear();
		}
		//}
	}

	protected abstract void compute(int superstepNo, List<M> messages, Q query);


	protected void sendMessageToAllOutgoingEdges(M message) {
		for (final Edge<E> edge : edges) {
			worker.sendVertexMessage(edge.TargetVertexId, message);
		}
	}

	protected void sendMessageToVertex(M message, int sendTo) {
		worker.sendVertexMessage(sendTo, message);
	}

	protected void sendMessageToVertices(M message, Collection<Integer> sendTo) {
		for (final Integer st : sendTo) {
			worker.sendVertexMessage(st, message);
		}
	}


	protected void voteVertexHalt() {
		vertexInactive = true;
	}

	public boolean isActive(int queryId) {
		return !(vertexInactive && queryMessagesNextSuperstep.get(queryId).isEmpty());
	}


	public void setDefaultValue(V dv) {
		vertexDefaultValue = dv;
	}

	public V getValue(int queryId) {
		return queryValues.get(queryId);
	}

	public void setValue(V value, int queryId) {
		this.queryValues.put(queryId, value);
	}


	public List<Edge<E>> getEdges() {
		return edges;
	}


	public void setEdges(List<Edge<E>> edges) {
		this.edges = edges;
	}


	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "_" + ID + "(" + queryValues + ")," + edges;
	}

	//	private String getValueString(int queryId) {
	//		V value = getValue(queryId);
	//		if (value == null) return "";
	//		return value.getString();
	//	}
}
