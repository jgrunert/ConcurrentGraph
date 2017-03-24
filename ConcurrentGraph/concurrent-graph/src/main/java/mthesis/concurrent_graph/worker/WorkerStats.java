package mthesis.concurrent_graph.worker;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;

/**
 * Statistics per worker, query independent.
 */
public class WorkerStats {

	private static final int WorkerStatsMaxBytes = 2048;
	private final byte[] workerStatsBuffer = new byte[WorkerStatsMaxBytes];

	// Direct variables for quick access of frequently changed variables
	public long IdleTime;
	public long QueryWaitTime;
	public long HandleMessagesTime;
	public long ComputeTime;
	public long BarrierStartWaitTime;
	public long BarrierFinishWaitTime;
	public long BarrierVertexMoveTime;

	public long ActiveVertices;
	public long MessagesTransmittedLocal;
	public long MessagesSentUnicast;
	public long MessagesSentBroadcast;
	public long MessageBucketsSentUnicast;
	public long MessageBucketsSentBroadcast;
	public long MessagesReceivedWrongVertex;
	public long MessagesReceivedCorrectVertex;
	public long DiscoveredNewVertexMachines;
	public long MoveSendVertices;
	public long MoveRecvVertices;

	// Detailed stats
	public long MoveSendVerticesMessages; // TODO

	//	public final HashMap<String, Long> OtherStats;


	public WorkerStats() {
		//		OtherStats = new HashMap<>();
	}

	public WorkerStats(ByteString bytesString) {
		super();
		ByteBuffer bytes = ByteBuffer.allocate(WorkerStatsMaxBytes);
		bytesString.copyTo(bytes);
		bytes.position(0);

		ActiveVertices = bytes.getLong();
		IdleTime = bytes.getLong();
		QueryWaitTime = bytes.getLong();
		HandleMessagesTime = bytes.getLong();
		ComputeTime = bytes.getLong();
		BarrierStartWaitTime = bytes.getLong();
		BarrierFinishWaitTime = bytes.getLong();
		BarrierVertexMoveTime = bytes.getLong();

		MessagesTransmittedLocal = bytes.getLong();
		MessagesSentUnicast = bytes.getLong();
		MessagesSentBroadcast = bytes.getLong();
		MessageBucketsSentUnicast = bytes.getLong();
		MessageBucketsSentBroadcast = bytes.getLong();
		MessagesReceivedWrongVertex = bytes.getLong();
		MessagesReceivedCorrectVertex = bytes.getLong();
		DiscoveredNewVertexMachines = bytes.getLong();
		MoveSendVertices = bytes.getLong();
		MoveRecvVertices = bytes.getLong();

		MoveSendVerticesMessages = bytes.getLong();

		//		int numOtherStats = bytes.getInt();
		//		OtherStats = new HashMap<>(numOtherStats);
		//		for (int i = 0; i < numOtherStats; i++) {
		//			OtherStats.put(bytes.getInt(), bytes.getLong());
		//		}
	}


	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putLong(ActiveVertices);
		buffer.putLong(IdleTime);
		buffer.putLong(QueryWaitTime);
		buffer.putLong(HandleMessagesTime);
		buffer.putLong(ComputeTime);
		buffer.putLong(BarrierStartWaitTime);
		buffer.putLong(BarrierFinishWaitTime);
		buffer.putLong(BarrierVertexMoveTime);

		buffer.putLong(MessagesTransmittedLocal);
		buffer.putLong(MessagesSentUnicast);
		buffer.putLong(MessagesSentBroadcast);
		buffer.putLong(MessageBucketsSentUnicast);
		buffer.putLong(MessageBucketsSentBroadcast);
		buffer.putLong(MessagesReceivedWrongVertex);
		buffer.putLong(MessagesReceivedCorrectVertex);
		buffer.putLong(DiscoveredNewVertexMachines);
		buffer.putLong(MoveSendVertices);
		buffer.putLong(MoveRecvVertices);

		buffer.putLong(MoveSendVerticesMessages);

		//		buffer.putInt(OtherStats.size());
		//		for (Entry<Integer, Long> stat : OtherStats.entrySet()) {
		//			buffer.putInt(stat.getKey());
		//			buffer.putLong(stat.getValue());
		//		}
	}

	public Map<String, Long> getStatsMap() {
		Map<String, Long> statsMap = new TreeMap<>();

		statsMap.put("ActiveVertices", ActiveVertices);
		statsMap.put("IdleTime", IdleTime);
		statsMap.put("QueryWaitTime", QueryWaitTime);
		statsMap.put("HandleMessagesTime", HandleMessagesTime);
		statsMap.put("ComputeTime", ComputeTime);
		statsMap.put("BarrierStartWaitTime", BarrierStartWaitTime);
		statsMap.put("BarrierFinishWaitTime", BarrierFinishWaitTime);
		statsMap.put("BarrierVertexMoveTime", BarrierVertexMoveTime);

		statsMap.put("MessagesTransmittedLocal", MessagesTransmittedLocal);
		statsMap.put("MessagesSentUnicast", MessagesSentUnicast);
		statsMap.put("MessagesSentBroadcast", MessagesSentBroadcast);
		statsMap.put("MessageBucketsSentUnicast", MessageBucketsSentUnicast);
		statsMap.put("MessageBucketsSentBroadcast", MessageBucketsSentBroadcast);
		statsMap.put("MessagesReceivedWrongVertex", MessagesReceivedWrongVertex);
		statsMap.put("MessagesReceivedCorrectVertex", MessagesReceivedCorrectVertex);
		statsMap.put("DiscoveredNewVertexMachines", DiscoveredNewVertexMachines);
		statsMap.put("MoveSendVertices", MoveSendVertices);
		statsMap.put("MoveRecvVertices", MoveRecvVertices);

		statsMap.put("MoveSendVerticesMessages", MoveSendVerticesMessages);

		return statsMap;
	}


	public WorkerStatSample getSample(long sampleTime) {
		ByteBuffer buffer = ByteBuffer.wrap(workerStatsBuffer);
		writeToBuffer(buffer);
		int byteCount = buffer.position();
		buffer.position(0);
		ByteString bs = ByteString.copyFrom(buffer, byteCount);
		return WorkerStatSample.newBuilder().setTime(sampleTime).setStatsBytes(bs).build();
	}
}
