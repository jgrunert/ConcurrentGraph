package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.writable.BaseWritable;

public interface VertexWorkerInterface<M extends BaseWritable, G extends BaseQueryGlobalValues> {

	void sendVertexMessage(int dstVertex, M content);
}
