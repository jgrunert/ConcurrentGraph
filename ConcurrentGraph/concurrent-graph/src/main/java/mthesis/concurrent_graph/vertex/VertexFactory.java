package mthesis.concurrent_graph.vertex;

import mthesis.concurrent_graph.writable.BaseWritable;

public abstract class VertexFactory<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> {
	public abstract AbstractVertex<V, E, M> newInstance(int id, VertexMessageSender<M> messageSender);
}
