package mthesis.concurrent_graph.vertex;

import mthesis.concurrent_graph.writable.BaseWritable;

public class VertexMessage<T extends BaseWritable> {
	public final int SuperstepNo;
	public final int SrcVertex;
	public final int DstVertex;
	public final T Content;

	public VertexMessage(int superstepNo, int srcVertex, int dstVertex, T content) {
		super();
		SuperstepNo = superstepNo;
		SrcVertex = srcVertex;
		DstVertex = dstVertex;
		Content = content;
	}
}
