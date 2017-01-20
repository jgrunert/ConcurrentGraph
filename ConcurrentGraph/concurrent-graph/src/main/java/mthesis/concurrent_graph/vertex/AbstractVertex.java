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
import mthesis.concurrent_graph.worker.WorkerQuery;
import mthesis.concurrent_graph.writable.BaseWritable;


public abstract class AbstractVertex<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public final int ID;

	// TODO More efficient datastructure? Arrays, re-use objects etc
	// value = (V[]) new Object[n + 1];
	private List<Edge<E>> edges;
	public Int2ObjectMap<V> queryValues = new Int2ObjectOpenHashMap<>(Settings.DEFAULT_QUERY_SLOTS);
	private V vertexDefaultValue = null;
	private Int2BooleanMap queryVertexInactive = new Int2BooleanOpenHashMap();

	public Int2ObjectMap<List<M>> queryMessagesThisSuperstep = new Int2ObjectOpenHashMap<>(Settings.DEFAULT_QUERY_SLOTS);
	public Int2ObjectMap<List<M>> queryMessagesNextSuperstep = new Int2ObjectOpenHashMap<>(Settings.DEFAULT_QUERY_SLOTS);

	private final VertexWorkerInterface<V, E, M, Q> worker;


	public AbstractVertex(int id, VertexWorkerInterface<V, E, M, Q> worker) {
		super();
		this.ID = id;
		this.worker = worker;
	}


	public void startQuery(int queryId) {
		//		queryValues.put(queryId, vertexDefaultValue != null ? worker.getVertexValueFactory().createClone(vertexDefaultValue) : null);
		//		queryVertexInactive.put(queryId, false);
		//		queryMessagesNextSuperstep.put(queryId, new ArrayList<>());
	}

	public void finishQuery(int queryId) {
		queryValues.remove(queryId);
		queryVertexInactive.remove(queryId);
		queryMessagesNextSuperstep.remove(queryId);
	}

	/**
	 * Called after barrier sync complete.
	 * Prepares received messages for next superstep.
	 * @return isActive
	 */
	public boolean finishSuperstep(int queryId) {
		List<M> messagesLast = queryMessagesThisSuperstep.get(queryId);
		List<M> messagesNext = queryMessagesNextSuperstep.get(queryId);
		if (messagesNext != null && !messagesNext.isEmpty()) {
			// Swap last and next lists, recycle as much as possible
			queryMessagesThisSuperstep.put(queryId, messagesNext);
			if (messagesLast == null)
				messagesLast = new ArrayList<>();
			else
				messagesLast.clear();
			queryMessagesNextSuperstep.put(queryId, messagesLast);
		}
		else if (messagesLast != null) {
			// Don't swap lists but clear messagesLast
			messagesLast.clear();
		}

		return !(queryVertexInactive.get(queryId) && (messagesNext == null || messagesNext.isEmpty()));
	}


	public void superstep(int superstepNo, WorkerQuery<M, Q> query) {
		int queryId = query.Query.QueryId;
		List<M> messagesThisSuperstep = queryMessagesThisSuperstep.get(queryId);

		if (!(queryVertexInactive.get(queryId) && (messagesThisSuperstep == null || messagesThisSuperstep.isEmpty()))) {
			//			if (messagesThisSuperstep == null) {
			//				messagesThisSuperstep = new ArrayList<>();
			//				queryMessagesNextSuperstep.put(query.QueryId, messagesThisSuperstep);
			//			}
			// Reset halt flag
			queryVertexInactive.remove(queryId);

			// Compute vertex
			compute(superstepNo, messagesThisSuperstep, query);
			if (messagesThisSuperstep != null)
				messagesThisSuperstep.clear();

			// Activate vertex for next superstep
			if (!(queryVertexInactive.get(queryId)))
				query.ActiveVerticesNext.put(ID, this);
		}
	}

	protected abstract void compute(int superstepNo, List<M> messages, WorkerQuery<M, Q> query);


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
		List<M> messagesThisSuperstep = queryMessagesThisSuperstep.get(queryId);
		return (!(queryVertexInactive.get(queryId) && (messagesThisSuperstep == null || messagesThisSuperstep.isEmpty())));
	}


	public void setDefaultValue(V dv) {
		vertexDefaultValue = dv;
	}

	public V getValue(int queryId) {
		V value = queryValues.get(queryId);
		return value != null ? value : vertexDefaultValue;
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

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		return obj instanceof AbstractVertex && ((AbstractVertex) obj).ID == ID;
	}

	@Override
	public int hashCode() {
		return ID;
	}

	//	private String getValueString(int queryId) {
	//		V value = getValue(queryId);
	//		if (value == null) return "";
	//		return value.getString();
	//	}
}
