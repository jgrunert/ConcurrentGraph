package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.Configuration;
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
		if (!Configuration.VERTEX_MESSAGE_POOLING)
			return new VertexMessage<>(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages, this, referenceCounter);

		VertexMessage<V, E, M, Q> message;
		synchronized (pool) {
			message = pool.poll();
		}
		if (message == null) {
			message = new VertexMessage<>(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages, this, referenceCounter);
		}
		else {
			message.setup(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages, referenceCounter);
		}
		return message;
	}

	public VertexMessage<V, E, M, Q> getPooledVertexMessage(ByteBuffer buffer, int referenceCounter) {
		if (!Configuration.VERTEX_MESSAGE_POOLING)
			return new VertexMessage<>(buffer, jobConfig, this, referenceCounter);

		VertexMessage<V, E, M, Q> message;
		synchronized (pool) {
			message = pool.poll();
		}
		if (message == null) {
			message = new VertexMessage<>(buffer, jobConfig, this, referenceCounter);
		}
		else {
			message.setup(buffer, jobConfig, referenceCounter);
		}
		return message;
	}


	public void freeVertexMessage(VertexMessage<V, E, M, Q> message, boolean freeMembers) {
		if (!Configuration.VERTEX_MESSAGE_POOLING)
			return;

		if (freeMembers) {
			for (Pair<Integer, M> vMsg : message.vertexMessages) {
				jobConfig.freePooledMessageValue(vMsg.second);
			}
		}
		synchronized (pool) {
			pool.add(message);
		}
	}
}
