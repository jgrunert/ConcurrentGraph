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
	public static MessageEnvelope Build(int superstepNo, int srcMachineId, int srcVertex, int dstVertex, int content) {
		return MessageEnvelope.newBuilder().setVertexMessage(
				VertexMessage.newBuilder()
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.setSrcVertex(srcVertex)
				.setDstVertex(dstVertex)
				.setContent(content)
				.build()).build();
	}
}
