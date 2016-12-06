package mthesis.concurrent_graph.communication;

public class ControlMessage {
	public final MessageType Type;
	public final int FromNode;
	public final int SuperstepNo;
	public final String Content;

	public ControlMessage(MessageType type, int fromNode, int superstepNo, String content) {
		super();
		Type = type;
		FromNode = fromNode;
		SuperstepNo = superstepNo;
		Content = content;
	}
}
