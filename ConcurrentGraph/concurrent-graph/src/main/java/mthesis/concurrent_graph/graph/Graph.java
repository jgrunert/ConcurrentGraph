package mthesis.concurrent_graph.graph;

import mthesis.concurrent_graph.writable.BaseWritable;

public interface Graph<V extends BaseWritable, E extends BaseWritable> {

	V getValue(int vertexId);

	void setValue(int vertexId, V value);

	Edge<E>[] getOutgoingEdges(int vertexId);
}
