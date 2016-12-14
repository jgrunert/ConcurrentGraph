package mthesis.concurrent_graph.worker;

import mthesis.concurrent_graph.writable.IntWritable;

public class GlobalObjects {
	private final IntWritable ActiveVertices = new IntWritable(0);
	private final IntWritable VertexCount = new IntWritable(0);

	public IntWritable getVertexCount() {
		return VertexCount;
	}

	public void setVertexCount(int vertexCount) {
		VertexCount.Value = vertexCount;
	}

	public IntWritable getActiveVertices() {
		return ActiveVertices;
	}

	public void setActiveVertices(int activeVertices) {
		ActiveVertices.Value = activeVertices;
	}
}
