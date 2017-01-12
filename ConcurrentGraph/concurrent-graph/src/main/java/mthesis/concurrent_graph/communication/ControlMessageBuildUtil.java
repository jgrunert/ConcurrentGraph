package mthesis.concurrent_graph.communication;

import java.util.List;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.AssignPartitionsMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerInitializedMessage;
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

	public static MessageEnvelope Build_Master_WorkerInitialize(int superstepNo, int srcMachineId, List<String> partitions) {
		final AssignPartitionsMessage assignPartitionsMsg = AssignPartitionsMessage.newBuilder().addAllPartitionFiles(partitions).build();
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Worker_Initialize)
				.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId)
				.setAssignPartitions(assignPartitionsMsg).build()).build();
	}

	public static MessageEnvelope Build_Master_QueryStart(int srcMachineId, BaseQueryGlobalValues query) {
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Query_Start)
				.setSuperstepNo(0)
				.setQueryValues(ByteString.copyFrom(query.getBytes()))
				.setSrcMachine(srcMachineId).build())
				.build();
	}
	
	public static MessageEnvelope Build_Master_QueryNextSuperstep(int superstepNo, int srcMachineId, BaseQueryGlobalValues query) {
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Query_Next_Superstep)
				.setQueryValues(ByteString.copyFrom(query.getBytes()))
				.setSuperstepNo(superstepNo)
				.setSrcMachine(srcMachineId).setQueryValues(ByteString.copyFrom(query.getBytes())).build())
				.build();
	}

	public static MessageEnvelope Build_Master_QueryFinish(int srcMachineId, BaseQueryGlobalValues query) {
		return MessageEnvelope.newBuilder().setControlMessage(ControlMessage.newBuilder()
				.setType(ControlMessageType.Master_Query_Finished)
				.setQueryValues(ByteString.copyFrom(query.getBytes()))
				.setSrcMachine(srcMachineId).build()).build();
	}

	public static MessageEnvelope Build_Worker_QuerySuperstepBarrier(int superstepNo, int srcMachineId, BaseQueryGlobalValues query) {
		return MessageEnvelope.newBuilder().setControlMessage(ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Query_Superstep_Barrier)
				.setQueryValues(ByteString.copyFrom(query.getBytes()))
				.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).build())
				.build();
	}

	public static MessageEnvelope Build_Worker_Initialized(int srcMachineId, int vertexCount) {
		final WorkerInitializedMessage workerInit = WorkerInitializedMessage.newBuilder()
				.setVertexCount(vertexCount).build();
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Worker_Initialized)
				.setWorkerInitialized(workerInit)
				.setSrcMachine(srcMachineId).build())
				.build();
	}
	
	public static MessageEnvelope Build_Worker_QuerySuperstepFinished(int superstepNo, int srcMachineId, SuperstepStats stats,
			BaseQueryGlobalValues localQuery) {
		final WorkerStatsMessage workerStats = WorkerStatsMessage.newBuilder()
				.setSentVertexMessagesLocal(stats.SentVertexMessagesLocal)
				.setSentVertexMessagesUnicast(stats.SentVertexMessagesUnicast)
				.setSentVertexMessagesBroadcast(stats.SentVertexMessagesBroadcast)
				.setSentVertexMessagesBuckets(stats.SentVertexMessagesBuckets)
				.setReceivedCorrectVertexMessages(stats.ReceivedCorrectVertexMessages)
				.setReceivedWrongVertexMessages(stats.ReceivedWrongVertexMessages)
				.setNewVertexMachinesDiscovered(stats.NewVertexMachinesDiscovered)
				.setTotalVertexMachinesDiscovered(stats.TotalVertexMachinesDiscovered).build();
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Worker_Query_Superstep_Finished)
						.setQueryValues(ByteString.copyFrom(localQuery.getBytes()))
						.setSuperstepNo(superstepNo)
						.setSrcMachine(srcMachineId).setWorkerStats(workerStats).build())
				.build();
	}

	public static MessageEnvelope Build_Worker_QueryFinished(int superstepNo, int srcMachineId, BaseQueryGlobalValues localQuery) {
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder()
				.setQueryValues(ByteString.copyFrom(localQuery.getBytes()))
				.setType(ControlMessageType.Worker_Query_Finished)
				.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).build()).build();
	}
}
