package mthesis.concurrent_graph.communication;

public class VertexMessage {
	public final int From;
	public final int To;
	public final int SuperstepNo;
	public final String Content;	

	public VertexMessage(int from, int to, int superstepNo, String content) {
		super();
		From = from;
		To = to;
		SuperstepNo = superstepNo;
		Content = content;
	}
}
