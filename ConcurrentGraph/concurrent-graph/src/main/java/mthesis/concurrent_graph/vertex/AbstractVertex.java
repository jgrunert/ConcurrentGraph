package mthesis.concurrent_graph.vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2BooleanMap;
import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
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
	private Int2BooleanMap queryVertexInactive = new Int2BooleanOpenHashMap();

	private final VertexWorkerInterface<V, E, M, Q> worker;


	public AbstractVertex(int id, VertexWorkerInterface<V, E, M, Q> worker) {
		super();
		this.ID = id;
		this.worker = worker;
	}

	public void startQuery(int queryId) {
		queryValues.put(queryId, vertexDefaultValue != null ? worker.getVertexValueFactory().createClone(vertexDefaultValue) : null);
		queryVertexInactive.put(queryId, false);
		queryMessagesNextSuperstep.put(queryId, new ArrayList<>());
	}

	public void finishQuery(int queryId) {
		queryValues.remove(queryId);
		queryVertexInactive.remove(queryId);
		queryMessagesNextSuperstep.remove(queryId);
	}


	public void superstep(int superstepNo, Q query) {
		List<M> messagesNextSuperstep = queryMessagesNextSuperstep.get(query.QueryId);

		if (!(queryVertexInactive.get(query.QueryId) && messagesNextSuperstep.isEmpty())) {
			compute(superstepNo, messagesNextSuperstep, query);
			messagesNextSuperstep.clear();
		}
	}

	protected abstract void compute(int superstepNo, List<M> messages, Q query);


	protected void sendMessageToAllOutgoingEdges(M message, int queryId) {
		for (final Edge<E> edge : edges) {
			worker.sendVertexMessage(edge.TargetVertexId, message, queryId);
		}
	}

	protected void sendMessageToVertex(M message, int sendTo, int queryId) {
		worker.sendVertexMessage(sendTo, message, queryId);
	}

	protected void sendMessageToVertices(M message, Collection<Integer> sendTo, int queryId) {
		for (final Integer st : sendTo) {
			worker.sendVertexMessage(st, message, queryId);
		}
	}


	protected void voteVertexHalt(int queryId) {
		queryVertexInactive.put(queryId, true);
	}

	public boolean isActive(int queryId) {
		return !(queryVertexInactive.get(queryId) && queryMessagesNextSuperstep.get(queryId).isEmpty());
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
