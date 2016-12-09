package mthesis.concurrent_graph.communication;

public class ControlMessageOld extends Message {
	public final int Content1;//TODO Type
	public final int Content2;//TODO Type

	public ControlMessageOld(MessageType type, int superstepNo, int fromNode, int content1, int content2) {
		super(type, superstepNo, fromNode);
		Content1 = content1;
		Content2 = content2;
	}


	@Override
	public String toString() {
		return "ControlMessage(" + Type + " SStep=" + SuperstepNo + " From=" + FromNode + " " + Content1 + " " + Content2 + ")";
	}
}
