package mthesis.concurrent_graph.vertex;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;

public abstract class VertexFactory<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	protected final JobConfiguration<V, E, M, Q> jobConfig;

	public VertexFactory(JobConfiguration<V, E, M, Q> jobConfig) {
		super();
		this.jobConfig = jobConfig;
	}


	public abstract AbstractVertex<V, E, M, Q> newInstance(int id, VertexWorkerInterface<V, E, M, Q> messageSender);

	public abstract AbstractVertex<V, E, M, Q> newInstance(ByteBuffer bufferToRead,
			VertexWorkerInterface<V, E, M, Q> worker);
}
