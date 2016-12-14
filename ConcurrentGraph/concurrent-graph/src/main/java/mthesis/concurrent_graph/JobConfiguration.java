package mthesis.concurrent_graph;

import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public abstract class JobConfiguration<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> {
	//	public abstract Class<?> getVertexFactoryClass();
	//	public abstract Class<?> getVertexClass();
	//	public abstract Class<?> getVertexValueClass();
	//	public abstract Class<?> getEdgeValueClass();

	public abstract VertexFactory<V, E, M> getVertexFactory();
	public abstract BaseWritableFactory<V> getVertexValueFactory();
	public abstract BaseWritableFactory<E> getEdgeValueFactory();
	public abstract BaseWritableFactory<M> getMessageValueFactory();
}
