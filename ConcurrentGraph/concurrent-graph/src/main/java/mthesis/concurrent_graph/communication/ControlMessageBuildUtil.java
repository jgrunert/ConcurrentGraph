package mthesis.concurrent_graph.communication;

import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.worker.SuperstepStats;


/**
 * Utilities for ControlMessage building
 * 
 * @author Joans Grunert
 *
 */
public class ControlMessageBuildUtil {
	public static MessageEnvelope Build_Master_Next_Superstep(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Next_Superstep)
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.build()).build();
	}

	public static MessageEnvelope Build_Master_Finish(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Finish)
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.build()).build();
	}

	public static MessageEnvelope Build_Worker_Superstep_Barrier(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Barrier)
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.build()).build();
	}

	public static MessageEnvelope Build_Worker_Superstep_Finished(int superstepNo, int srcMachineId, SuperstepStats stats) {
		final WorkerStatsMessage workerStats = WorkerStatsMessage.newBuilder()
				.setActiveVertices(stats.ActiveVertices)
				.setSentControlMessages(stats.SentControlMessages)
				.setSentVertexMessagesLocal(stats.SentVertexMessagesLocal)
				.setSentVertexMessagesUnicast(stats.SentVertexMessagesUnicast)
				.setSentVertexMessagesBroadcast(stats.SentVertexMessagesBroadcast)
				.setReceivedCorrectVertexMessages(stats.ReceivedCorrectVertexMessages)
				.setReceivedWrongVertexMessages(stats.ReceivedWrongVertexMessages)
				.setNewVertexMachinesDiscovered(stats.NewVertexMachinesDiscovered)
				.setTotalVertexMachinesDiscovered(stats.TotalVertexMachinesDiscovered)
				.build();
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Finished)
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.setWorkerStats(workerStats)
				.build()).build();
	}

	public static MessageEnvelope Build_Worker_Finished(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(
				ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Finished)
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId)
				.build()).build();
	}
}
