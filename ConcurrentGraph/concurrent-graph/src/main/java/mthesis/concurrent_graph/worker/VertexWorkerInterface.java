package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.writable.BaseWritable;

public interface VertexWorkerInterface<M extends BaseWritable, G extends QueryGlobalValues> {

	void sendVertexMessage(int dstVertex, M content);

	G getLocalQueryValues();

	G getGlobalQueryValues();
}
