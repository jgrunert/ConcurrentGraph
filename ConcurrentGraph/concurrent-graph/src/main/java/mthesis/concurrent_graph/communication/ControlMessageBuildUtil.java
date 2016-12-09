package mthesis.concurrent_graph.communication;

import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;


/**
 * Utilities for ControlMessage building
 * 
 * @author Joans Grunert
 *
 */
public class ControlMessageBuildUtil {
	public static ControlMessage Build_Master_Next_Superstep(int superstepNo, int senderId) {
		return ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Next_Superstep)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build();
	}

	public static ControlMessage Build_Master_Finish(int superstepNo, int senderId) {
		return ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Finish)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build();
	}

	public static ControlMessage Build_Worker_Superstep_Barrier(int superstepNo, int senderId) {
		return ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Barrier)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build();
	}

	public static ControlMessage Build_Worker_Superstep_Finished(int superstepNo, int senderId, int activeVertices, int superstepMessagesSent) {
		return ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Finished)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build();
	}

	public static ControlMessage Build_Worker_Finished(int superstepNo, int senderId) {
		return ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Finished)
				.setSuperstepNo(superstepNo)
				.setFromNode(senderId)
				.build();
	}
}
