package mthesis.concurrent_graph.communication;

/**
 * Abstract base class for all messages
 * 
 * @author Jonas Grunert
 *
 */
public abstract class Message {
	public final MessageType Type;
	public final int SuperstepNo;
	public final int FromNode;
	// TODO ToNode?

	public Message(MessageType type, int superstepNo, int fromNode) {
		super();
		Type = type;
		FromNode = fromNode;
		SuperstepNo = superstepNo;
	}
}
