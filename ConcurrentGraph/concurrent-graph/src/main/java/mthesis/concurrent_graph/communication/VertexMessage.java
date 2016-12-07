package mthesis.concurrent_graph.communication;

public class VertexMessage {
	public final int FromNode;
	public final int FromVertex;
	public final int ToVertex;
	public final int SuperstepNo;
	public final String Content;

	public VertexMessage(int fromNode, int from, int to, int superstepNo, String content) {
		super();
		FromNode = fromNode;
		FromVertex = from;
		ToVertex = to;
		SuperstepNo = superstepNo;
		Content = content;
	}


	@Override
	public String toString() {
		return "VertexMessage(" + SuperstepNo + " " + FromVertex + " " + ToVertex + " " + Content + ")";
	}
}
