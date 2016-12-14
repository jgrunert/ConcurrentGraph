package mthesis.concurrent_graph.vertex;

import mthesis.concurrent_graph.writable.BaseWritable;

public interface VertexMessageSender<M extends BaseWritable> {
	void sendVertexMessage(int srcVertex, int dstVertex, M content);
}
