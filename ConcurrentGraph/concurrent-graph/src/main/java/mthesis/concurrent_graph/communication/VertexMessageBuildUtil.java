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
	public static MessageEnvelope BuildWithoutContent(int superstepNo, int srcMachineId, int srcVertex, int dstVertex, boolean broadcastFlag) {
		final VertexMessage.Builder vertesMsg = VertexMessage.newBuilder()
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.setSrcVertex(srcVertex)
				.setDstVertex(dstVertex);
		if(broadcastFlag)
			vertesMsg.setBroadcastFlat(true);
		return MessageEnvelope.newBuilder().setVertexMessage(vertesMsg.build()).build();
	}

	public static MessageEnvelope BuildWithContent(int superstepNo, int srcMachineId, int srcVertex, int dstVertex, boolean broadcastFlag, int content) {
		final VertexMessage.Builder vertesMsg = VertexMessage.newBuilder()
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.setSrcVertex(srcVertex)
				.setDstVertex(dstVertex)
				.setContent(content);
		if(broadcastFlag)
			vertesMsg.setBroadcastFlat(true);
		return MessageEnvelope.newBuilder().setVertexMessage(vertesMsg.build()).build();
	}
}
