package mthesis.concurrent_graph.communication;

public class ControlMessage {
	public final MessageType Type;
	public final int FromNode;
	public final int SuperstepNo;

	public final int Content1;//TODO Type
	public final int Content2;//TODO Type

	public ControlMessage(MessageType type, int fromNode, int superstepNo, int content1, int content2) {
		super();
		Type = type;
		FromNode = fromNode;
		SuperstepNo = superstepNo;
		Content1 = content1;
		Content2 = content2;
	}
}
