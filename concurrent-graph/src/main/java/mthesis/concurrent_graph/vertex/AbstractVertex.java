package mthesis.concurrent_graph.vertex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.worker.VertexMoveFailureException;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.worker.WorkerQuery;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;


public abstract class AbstractVertex<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractVertex.class);

	public final int ID;

	// TODO More efficient datastructure? Arrays or fastutil map
	private List<Edge<E>> edges;
	private V vertexDefaultValue = null;
	public final Int2ObjectMap<V> queryValues = new Int2ObjectOpenHashMap<>(Configuration.DEFAULT_QUERY_SLOTS);
	// Queries this vertex is inactive for
	private IntSet queriesVertexInactive = new IntOpenHashSet(Configuration.DEFAULT_QUERY_SLOTS);

	// Message double buffer
	public Int2ObjectMap<List<M>> queryMessagesThisSuperstep = new Int2ObjectOpenHashMap<>(Configuration.DEFAULT_QUERY_SLOTS);
	public Int2ObjectMap<List<M>> queryMessagesNextSuperstep = new Int2ObjectOpenHashMap<>(Configuration.DEFAULT_QUERY_SLOTS);
	//	public Int2IntMap querySuperstepNumbers = new Int2IntOpenHashMap(Configuration.DEFAULT_QUERY_SLOTS); // For consistency check

	public long lastSuperstepTime;

	private final VertexWorkerInterface<V, E, M, Q> worker;


	public AbstractVertex(int id, VertexWorkerInterface<V, E, M, Q> worker) {
		super();
		this.ID = id;
		this.worker = worker;
	}

	public AbstractVertex(ByteBuffer bufferToRead, VertexWorkerInterface<V, E, M, Q> worker,
			JobConfiguration<V, E, M, Q> jobConfig) {
		super();
		this.worker = worker;
		BaseWritableFactory<V> vertexValueFactory = jobConfig.getVertexValueFactory();
		BaseWritableFactory<E> edgeValueFactory = jobConfig.getEdgeValueFactory();
		BaseWritableFactory<M> messageValueFactory = jobConfig.getMessageValueFactory();

		this.ID = bufferToRead.getInt();

		int edgeCount = bufferToRead.getInt();
		edges = new ArrayList<>(edgeCount);
		for (int i = 0; i < edgeCount; i++) {
			edges.add(new Edge<>(bufferToRead.getInt(), edgeValueFactory.createFromBytes(bufferToRead)));
		}

		if (bufferToRead.get() == 0)
			vertexDefaultValue = vertexValueFactory.createFromBytes(bufferToRead);

		int queryValuesCount = bufferToRead.getInt();
		for (int i = 0; i < queryValuesCount; i++) {
			int key = bufferToRead.getInt();
			V value;
			if (bufferToRead.get() == 0)
				value = vertexValueFactory.createFromBytes(bufferToRead);
			else
				value = null;
			queryValues.put(key, value);
		}

		int queriesVertexInactiveCount = bufferToRead.getInt();
		for (int i = 0; i < queriesVertexInactiveCount; i++) {
			queriesVertexInactive.add(bufferToRead.getInt());
		}

		int queryMessagesThisSuperstepCount = bufferToRead.getInt();
		for (int i = 0; i < queryMessagesThisSuperstepCount; i++) {
			int key = bufferToRead.getInt();
			int valueCount = bufferToRead.getInt();
			List<M> msgs = new ArrayList<>(valueCount);
			for (int iV = 0; iV < valueCount; iV++) {
				msgs.add(messageValueFactory.createFromBytes(bufferToRead));
			}
			queryMessagesThisSuperstep.put(key, msgs);
		}

		//		int queryMessagesNextSuperstepCount = bufferToRead.getInt();
		//		for (int i = 0; i < queryMessagesNextSuperstepCount; i++) {
		//			int key = bufferToRead.getInt();
		//			int valueCount = bufferToRead.getInt();
		//			List<M> msgs = new ArrayList<>(valueCount);
		//			for (int iV = 0; iV < valueCount; iV++) {
		//				msgs.add(messageValueFactory.createFromBytes(bufferToRead));
		//			}
		//			queryMessagesNextSuperstep.put(key, msgs);
		//		}

		//		int querSuperstepNumbersCount = bufferToRead.getInt();
		//		for (int i = 0; i < querSuperstepNumbersCount; i++) {
		//			int key = bufferToRead.getInt();
		//			int value = bufferToRead.getInt();
		//			querySuperstepNumbers.put(key, value);
		//		}
	}

	/**
	 * Writes vertices to a buffer to send
	 * @throws VertexMoveFailureException
	 */
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(ID);

		buffer.putInt(edges.size());
		for (Edge<E> e : edges) {
			buffer.putInt(e.TargetVertexId);
			e.Value.writeToBuffer(buffer);
		}

		buffer.put(vertexDefaultValue != null ? (byte) 0 : (byte) 1);
		if (vertexDefaultValue != null)
			vertexDefaultValue.writeToBuffer(buffer);

		buffer.putInt(queryValues.size());
		for (Entry<Integer, V> qv : queryValues.entrySet()) {
			buffer.putInt(qv.getKey());
			V value = qv.getValue();
			if (qv.getValue() != null) {
				buffer.put((byte) 0);
				value.writeToBuffer(buffer);
			}
			else
				buffer.put((byte) 1);
		}

		buffer.putInt(queriesVertexInactive.size());
		for (Integer qi : queriesVertexInactive) {
			buffer.putInt(qi);
		}

		buffer.putInt(queryMessagesThisSuperstep.size());
		for (Entry<Integer, List<M>> qMsgs : queryMessagesThisSuperstep.entrySet()) {
			buffer.putInt(qMsgs.getKey());
			buffer.putInt(qMsgs.getValue().size());
			for (M msg : qMsgs.getValue()) {
				msg.writeToBuffer(buffer);
			}
		}

		for (Entry<Integer, List<M>> qMsgs : queryMessagesNextSuperstep.entrySet()) {
			if (!qMsgs.getValue().isEmpty()) {
				logger.warn(
						"Will not send vertex messages for next superstep - vertices to send shouldnt have these. Query" + qMsgs.getKey()
						+ " vertex " + ID);
				break;
			}
		}
		//		buffer.putInt(queryMessagesNextSuperstep.size());
		//		for (Entry<Integer, List<M>> qMsgs : queryMessagesNextSuperstep.entrySet()) {
		//			buffer.putInt(qMsgs.getKey());
		//			buffer.putInt(qMsgs.getValue().size());
		//			for (M msg : qMsgs.getValue()) {
		//				msg.writeToBuffer(buffer);
		//			}
		//		}

		//		buffer.putInt(querySuperstepNumbers.size());
		//		for (Entry<Integer, Integer> qMsgs : querySuperstepNumbers.entrySet()) {
		//			buffer.putInt(qMsgs.getKey());
		//			buffer.putInt(qMsgs.getValue());
		//		}
	}

	public void finishQuery(int queryId) {
		queryValues.remove(queryId);
		queriesVertexInactive.remove(queryId);
		queryMessagesThisSuperstep.remove(queryId);
		queryMessagesNextSuperstep.remove(queryId);
		//		querySuperstepNumbers.remove(queryId);
	}

	/**
	 * Called after barrier sync complete.
	 * Prepares received messages for next superstep, swaps double buffer.
	 * @return isActive
	 */
	public void prepareForNextSuperstep(Integer queryId) {
		List<M> messagesLast = queryMessagesThisSuperstep.get(queryId);
		List<M> messagesNext = queryMessagesNextSuperstep.get(queryId);

		// Swapping/clearing
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
	}


	public void superstep(int superstepNo, WorkerQuery<V, E, M, Q> query, boolean forceCompute) {
		int queryId = query.Query.QueryId;
		lastSuperstepTime = WorkerMachine.lastUpdateTime;
		List<M> messagesThisSuperstep = queryMessagesThisSuperstep.get(queryId);

		if (forceCompute
				|| !(queriesVertexInactive.contains(queryId) && (messagesThisSuperstep == null || messagesThisSuperstep.isEmpty()))) {
			//			if (messagesThisSuperstep == null) {
			//				messagesThisSuperstep = new ArrayList<>();
			//				queryMessagesNextSuperstep.put(query.QueryId, messagesThisSuperstep);
			//			}
			// Reset halt flag
			queriesVertexInactive.remove(queryId);

			// Compute vertex
			compute(superstepNo, messagesThisSuperstep, query);
			if (messagesThisSuperstep != null) {
				// Free and clear messages
				//				for (M msg : messagesThisSuperstep) {
				//					worker.freePooledMessageValue(msg);
				//				}
				messagesThisSuperstep.clear();
			}

			// Activate vertex for next superstep
			if (!(queriesVertexInactive.contains(queryId)))
				query.ActiveVerticesNext.put(ID, this);
		}
	}

	protected abstract void compute(int superstepNo, List<M> messages, WorkerQuery<V, E, M, Q> query);


	//	protected M getNewMessage() {
	//		return worker.getNewMessage();
	//	}
	//
	//	protected void freePooledMessageValue(M message) {
	//		worker.freePooledMessageValue(message);
	//	}

	protected void sendMessageToAllOutgoingEdges(M message, WorkerQuery<V, E, M, Q> query) {
		for (final Edge<E> edge : edges) {
			worker.sendVertexMessage(edge.TargetVertexId, message, query);
		}
	}

	protected void sendMessageToVertex(M message, int sendTo, WorkerQuery<V, E, M, Q> query) {
		worker.sendVertexMessage(sendTo, message, query);
	}

	protected void sendMessageToVertices(M message, Collection<Integer> sendTo, WorkerQuery<V, E, M, Q> query) {
		for (final Integer st : sendTo) {
			worker.sendVertexMessage(st, message, query);
		}
	}


	protected void voteVertexHalt(int queryId) {
		queriesVertexInactive.add(queryId);
	}

	//	public boolean isActive(int queryId) {
	//		List<M> messagesThisSuperstep = queryMessagesThisSuperstep.get(queryId);
	//		return (!(queryVertexInactive.get(queryId) && (messagesThisSuperstep == null || messagesThisSuperstep.isEmpty())));
	//	}


	public void setDefaultValue(V dv) {
		vertexDefaultValue = dv;
	}

	public V getValue(int queryId) {
		V value = queryValues.get(queryId);
		return value != null ? value : vertexDefaultValue;
	}

	public V getOutputValue(int queryId) {
		return getValue(queryId);
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


	public int getBufferedMessageCount() {
		int msgCount = 0;
		for (List<M> qMsgs : queryMessagesThisSuperstep.values()) {
			msgCount += qMsgs.size();
		}
		for (List<M> qMsgs : queryMessagesNextSuperstep.values()) {
			msgCount += qMsgs.size();
		}
		return msgCount;
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
