package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public interface VertexWorkerInterface<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	void sendVertexMessage(int dstVertex, M content, WorkerQuery<V, E, M, Q> query);

	BaseWritableFactory<V> getVertexValueFactory();


	//	M getNewMessage();
	//
	//	void freePooledMessageValue(M message);
}
