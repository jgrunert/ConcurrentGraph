package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.AssignPartitionsMessage;
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

	private static final int QueryGlobalValuesBufferSize = 1024;

	public static MessageEnvelope Build_Master_Startup(int superstepNo, int srcMachineId, List<String> partitions) {
		final AssignPartitionsMessage assignPartitionsMsg = AssignPartitionsMessage.newBuilder().addAllPartitionFiles(partitions).build();
		return MessageEnvelope.newBuilder().setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Master_Next_Superstep)
				.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).setAssignPartitions(assignPartitionsMsg).build()).build();
	}

	public static MessageEnvelope Build_Master_Next_Superstep(int superstepNo, int srcMachineId, QueryGlobalValues globalValues) {
		ByteBuffer globalValuesBuffer = ByteBuffer.allocate(QueryGlobalValuesBufferSize); // TODO Dont re allocate?
		globalValues.writeToBuffer(globalValuesBuffer);
		globalValuesBuffer.position(0);

		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Master_Next_Superstep).setSuperstepNo(superstepNo)
						.setSrcMachine(srcMachineId).setQueryGlobalValues(ByteString.copyFrom(globalValuesBuffer)).build())
				.build();
	}

	public static MessageEnvelope Build_Master_Finish(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Master_Finish)
				.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).build()).build();
	}

	public static MessageEnvelope Build_Worker_Superstep_Barrier(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(ControlMessage.newBuilder()
				.setType(ControlMessageType.Worker_Superstep_Barrier).setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).build())
				.build();
	}

	public static MessageEnvelope Build_Worker_Superstep_Finished(int superstepNo, int srcMachineId, SuperstepStats stats,
			int vertexCount) {
		final WorkerStatsMessage workerStats = WorkerStatsMessage.newBuilder().setVertexCount(vertexCount)
				.setActiveVertices(stats.ActiveVertices).setSentControlMessages(stats.SentControlMessages)
				.setSentVertexMessagesLocal(stats.SentVertexMessagesLocal).setSentVertexMessagesUnicast(stats.SentVertexMessagesUnicast)
				.setSentVertexMessagesBroadcast(stats.SentVertexMessagesBroadcast)
				.setSentVertexMessagesBuckets(stats.SentVertexMessagesBuckets)
				.setReceivedCorrectVertexMessages(stats.ReceivedCorrectVertexMessages)
				.setReceivedWrongVertexMessages(stats.ReceivedWrongVertexMessages)
				.setNewVertexMachinesDiscovered(stats.NewVertexMachinesDiscovered)
				.setTotalVertexMachinesDiscovered(stats.TotalVertexMachinesDiscovered).build();
		return MessageEnvelope.newBuilder()
				.setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Worker_Superstep_Finished)
						.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).setWorkerStats(workerStats).build())
				.build();
	}

	public static MessageEnvelope Build_Worker_Finished(int superstepNo, int srcMachineId) {
		return MessageEnvelope.newBuilder().setControlMessage(ControlMessage.newBuilder().setType(ControlMessageType.Worker_Finished)
				.setSuperstepNo(superstepNo).setSrcMachine(srcMachineId).build()).build();
	}
}
