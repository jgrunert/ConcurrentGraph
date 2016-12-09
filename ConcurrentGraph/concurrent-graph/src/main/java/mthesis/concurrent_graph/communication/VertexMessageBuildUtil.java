package mthesis.concurrent_graph.communication;

import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.Messages.VertexMessage;


/**
 * Utilities for ControlMessage building
 * 
 * @author Joans Grunert
 *
 */
public class VertexMessageBuildUtil {
	public static MessageEnvelope Build(int superstepNo, int fromNode, int fromVertex, int toVertex, int content) {
		return MessageEnvelope.newBuilder().setVertexMessage(
				VertexMessage.newBuilder()
				.setSuperstepNo(superstepNo)
				.setFromNode(fromNode)
				.setFromVertex(fromVertex)
				.setToVertex(toVertex)
				.setContent(content)
				.build()).build();
	}
}
