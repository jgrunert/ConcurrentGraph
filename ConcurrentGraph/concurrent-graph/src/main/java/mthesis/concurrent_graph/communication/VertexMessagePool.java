package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.BaseWritable;

public class VertexMessagePool<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	private final LinkedList<VertexMessage<V, E, M, Q>> pool = new LinkedList<>();
	private final JobConfiguration<V, E, M, Q> jobConfig;

	public VertexMessagePool(JobConfiguration<V, E, M, Q> jobConfig) {
		this.jobConfig = jobConfig;
	}


	public VertexMessage<V, E, M, Q> getPooledVertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages, int referenceCounter) {
		VertexMessage<V, E, M, Q> message;
		synchronized (pool) {
			message = pool.poll();
		}
		if (message == null) {
			message = new VertexMessage<>(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages, this);
		}
		else {
			message.setup(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages);
		}
		return message;
	}

	public VertexMessage<V, E, M, Q> getPooledVertexMessage(ByteBuffer buffer, BaseWritable.BaseWritableFactory<M> vertexMessageFactory,
			int referenceCounter) {
		VertexMessage<V, E, M, Q> message;
		synchronized (pool) {
			message = pool.poll();
		}
		if (message == null) {
			message = new VertexMessage<>(buffer, vertexMessageFactory, this);
		}
		else {
			message.setup(buffer, vertexMessageFactory);
		}
		return message;
	}


	public void freeVertexMessage(VertexMessage<V, E, M, Q> message) {
		synchronized (pool) {
			pool.add(message);
		}
	}
}
