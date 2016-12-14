package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.writable.BaseWritable;

public interface VertexWorkerInterface<M extends BaseWritable> {
	void sendVertexMessage(int srcVertex, int dstVertex, M content);
	GlobalObjects getGlobalObjects();
}
