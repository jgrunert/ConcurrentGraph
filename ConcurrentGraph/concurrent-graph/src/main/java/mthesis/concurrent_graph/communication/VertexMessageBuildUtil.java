package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.Messages.VertexMessageTransport;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Utilities for ControlMessage building
 * 
 * @author Joans Grunert
 *
 */
public class VertexMessageBuildUtil {
	public static MessageEnvelope BuildWithoutContent(int superstepNo, int srcMachineId, int srcVertex, int dstVertex) {
		final VertexMessageTransport.Builder vertesMsg =VertexMessageTransport.newBuilder()
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.setSrcVertex(srcVertex)
				.setDstVertex(dstVertex);
		return MessageEnvelope.newBuilder().setVertexMessage(vertesMsg.build()).build();
	}

	public static MessageEnvelope BuildWithContent(int superstepNo, int srcMachineId, int srcVertex, int dstVertex, BaseWritable content) {
		final ByteBuffer contentBytes = content.GetBytes();
		contentBytes.position(0);
		final VertexMessageTransport.Builder vertesMsg = VertexMessageTransport.newBuilder()
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.setSrcVertex(srcVertex)
				.setDstVertex(dstVertex)
				.setContent(ByteString.copyFrom(contentBytes));
		return MessageEnvelope.newBuilder().setVertexMessage(vertesMsg.build()).build();
	}
}
