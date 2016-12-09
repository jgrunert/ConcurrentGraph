package mthesis.concurrent_graph.communication;

import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;


/**
 * Utilities for ControlMessage building
 * 
 * @author Joans Grunert
 *
 */
public class ControlMessageBuildUtil {
	public static MessageEnvelope Build_Master_Next_Superstep(int superstepNo, int senderId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Next_Superstep)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build()).build();
	}

	public static MessageEnvelope Build_Master_Finish(int superstepNo, int senderId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Finish)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build()).build();
	}

	public static MessageEnvelope Build_Worker_Superstep_Barrier(int superstepNo, int senderId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Barrier)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build()).build();
	}

	public static MessageEnvelope Build_Worker_Superstep_Finished(int superstepNo, int senderId, int activeVertices, int superstepMessagesSent) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Finished)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.setContent1(activeVertices)
				.setContent2(superstepMessagesSent)
				.build()).build();
	}

	public static MessageEnvelope Build_Worker_Finished(int superstepNo, int senderId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Finished)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build()).build();
	}
}
