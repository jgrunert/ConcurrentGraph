package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public interface VertexWorkerInterface<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, G extends BaseQueryGlobalValues> {

	void sendVertexMessage(int dstVertex, M content, int queryId);

	public abstract BaseWritableFactory<V> getVertexValueFactory();
}
