package mthesis.concurrent_graph.communication;

public class VertexMessage extends Message {
	public final int FromVertex;
	public final int ToVertex;
	public final int Content;//TODO Type

	public VertexMessage(int superstepNo, int fromNode, int fromVertex, int toVertex, int content) {
		super(MessageType.Vertex, superstepNo, fromNode);
		FromVertex = fromVertex;
		ToVertex = toVertex;
		Content = content;
	}


	@Override
	public String toString() {
		return "VertexMessage(" + SuperstepNo + " " + FromNode + " " + FromVertex + " " + ToVertex + " " + Content + ")";
	}
}
