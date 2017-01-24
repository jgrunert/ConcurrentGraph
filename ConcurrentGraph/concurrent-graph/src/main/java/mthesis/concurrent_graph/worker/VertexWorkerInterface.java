package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public interface VertexWorkerInterface<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	void sendVertexMessage(int dstVertex, M content, WorkerQuery<V, E, M, Q> query);

	public abstract BaseWritableFactory<V> getVertexValueFactory();
}
