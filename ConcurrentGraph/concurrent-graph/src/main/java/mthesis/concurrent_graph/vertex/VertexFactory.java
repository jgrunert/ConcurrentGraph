package mthesis.concurrent_graph.vertex;

import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;

public abstract class VertexFactory<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, G extends QueryGlobalValues> {

	public abstract AbstractVertex<V, E, M, G> newInstance(int id, VertexWorkerInterface<M, G> messageSender);
}
