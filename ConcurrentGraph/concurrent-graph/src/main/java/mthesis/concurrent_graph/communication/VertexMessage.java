package mthesis.concurrent_graph.communication;

public class VertexMessage {
	public final int SuperstepNo;
	public final int FromNode;
	public final int FromVertex;
	public final int ToVertex;
	public final int Content;//TODO Type

	public VertexMessage(int superstepNo, int fromNode, int fromVertex, int toVertex, int content) {
		super();
		FromNode = fromNode;
		FromVertex = fromVertex;
		ToVertex = toVertex;
		SuperstepNo = superstepNo;
		Content = content;
	}


	@Override
	public String toString() {
		return "VertexMessage(" + SuperstepNo + " " + FromNode + " " + FromVertex + " " + ToVertex + " " + Content + ")";
	}
}
