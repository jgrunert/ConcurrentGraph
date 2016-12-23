package mthesis.concurrent_graph;

import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public abstract class JobConfiguration<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, G extends QueryGlobalValues> {

	public abstract VertexFactory<V, E, M, G> getVertexFactory();

	public abstract BaseWritableFactory<V> getVertexValueFactory();

	public abstract BaseWritableFactory<E> getEdgeValueFactory();

	public abstract BaseWritableFactory<M> getMessageValueFactory();

	public abstract BaseWritableFactory<G> getGlobalValuesFactory();
}
