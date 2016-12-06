package mthesis.concurrent_graph.communication;

public class ControlMessage {
	public final int SuperstepNo;
	public final String Content;	

	public ControlMessage(int superstepNo, String content) {
		super();
		SuperstepNo = superstepNo;
		Content = content;
	}
}
