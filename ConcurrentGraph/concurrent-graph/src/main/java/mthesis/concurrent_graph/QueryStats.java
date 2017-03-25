package mthesis.concurrent_graph;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.ByteString;


/**
 * Statistics per query and worker, over all active queries.
 * All statistics are kept in simple variables to keep performance good.
 *
 * @author Jonas Grunert
 *
 */
public class QueryStats {

	// Message stats
	public long MessagesTransmittedLocal;
	public long MessagesSentUnicast;
	public long MessagesSentBroadcast;
	public long MessageBucketsSentUnicast;
	public long MessageBucketsSentBroadcast;
	public long MessagesReceivedWrongVertex;
	public long MessagesReceivedCorrectVertex;
	public long DiscoveredNewVertexMachines;

	// Query Worker states
	public long ComputeTime;
	public long StepFinishTime;
	public long IntersectCalcTime;
	public long UpdateVertexRegisters;
	public long RedirectedMessages;
	public long MoveSendVertices;
	public long MoveRecvVertices;
	public long MoveSendVerticesTime;
	public long MoveRecvVerticesTime;

	// Detailed stats
	public long MoveSendVerticesMessages; // TODO



	public QueryStats() {
	}

	public QueryStats(ByteString bytesString) {
		super();
		ByteBuffer bytes = ByteBuffer.allocate(getBytesLength());
		bytesString.copyTo(bytes);
		bytes.position(0);
		initFromBytes(bytes);
	}

	public QueryStats(ByteBuffer bytes) {
		super();
		initFromBytes(bytes);
	}

	public void initFromBytes(ByteBuffer bytes) {
		MessagesTransmittedLocal = bytes.getLong();
		MessagesSentUnicast = bytes.getLong();
		MessagesSentBroadcast = bytes.getLong();
		MessageBucketsSentUnicast = bytes.getLong();
		MessageBucketsSentBroadcast = bytes.getLong();
		MessagesReceivedWrongVertex = bytes.getLong();
		MessagesReceivedCorrectVertex = bytes.getLong();
		DiscoveredNewVertexMachines = bytes.getLong();

		ComputeTime = bytes.getLong();
		StepFinishTime = bytes.getLong();
		IntersectCalcTime = bytes.getLong();
		UpdateVertexRegisters = bytes.getLong();
		RedirectedMessages = bytes.getLong();
		MoveSendVertices = bytes.getLong();
		MoveSendVerticesTime = bytes.getLong();
		MoveRecvVertices = bytes.getLong();
		MoveRecvVerticesTime = bytes.getLong();

		MoveSendVerticesMessages = bytes.getLong();
	}


	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putLong(MessagesTransmittedLocal);
		buffer.putLong(MessagesSentUnicast);
		buffer.putLong(MessagesSentBroadcast);
		buffer.putLong(MessageBucketsSentUnicast);
		buffer.putLong(MessageBucketsSentBroadcast);
		buffer.putLong(MessagesReceivedWrongVertex);
		buffer.putLong(MessagesReceivedCorrectVertex);
		buffer.putLong(DiscoveredNewVertexMachines);

		buffer.putLong(ComputeTime);
		buffer.putLong(StepFinishTime);
		buffer.putLong(IntersectCalcTime);
		buffer.putLong(UpdateVertexRegisters);
		buffer.putLong(RedirectedMessages);
		buffer.putLong(MoveSendVertices);
		buffer.putLong(MoveRecvVertices);
		buffer.putLong(MoveSendVerticesTime);
		buffer.putLong(MoveRecvVerticesTime);

		buffer.putLong(MoveSendVerticesMessages);
	}


	public Map<String, Long> getStatsMap() {
		Map<String, Long> statsMap = new TreeMap<>();

		statsMap.put("MessagesTransmittedLocal", MessagesTransmittedLocal);
		statsMap.put("MessagesSentUnicast", MessagesSentUnicast);
		statsMap.put("MessagesSentBroadcast", MessagesSentBroadcast);
		statsMap.put("MessageBucketsSentUnicast", MessageBucketsSentUnicast);
		statsMap.put("MessageBucketsSentBroadcast", MessageBucketsSentBroadcast);
		statsMap.put("MessagesReceivedWrongVertex", MessagesReceivedWrongVertex);
		statsMap.put("MessagesReceivedCorrectVertex", MessagesReceivedCorrectVertex);
		statsMap.put("DiscoveredNewVertexMachines", DiscoveredNewVertexMachines);

		statsMap.put("ComputeTime", ComputeTime);
		statsMap.put("StepFinishTime", StepFinishTime);
		statsMap.put("IntersectCalcTime", IntersectCalcTime);
		statsMap.put("UpdateVertexRegisters", UpdateVertexRegisters);
		statsMap.put("RedirectedMessages", RedirectedMessages);
		statsMap.put("MoveSendVertices", MoveSendVertices);
		statsMap.put("MoveSendVerticesTime", MoveSendVerticesTime);
		statsMap.put("MoveRecvVertices", MoveRecvVertices);
		statsMap.put("MoveRecvVerticesTime", MoveRecvVerticesTime);

		statsMap.put("MoveSendVerticesMessages", MoveSendVerticesMessages);

		return statsMap;
	}


	public void combine(QueryStats v) {
		MessagesTransmittedLocal += v.MessagesTransmittedLocal;
		MessagesSentUnicast += v.MessagesSentUnicast;
		MessagesSentBroadcast += v.MessagesSentBroadcast;
		MessageBucketsSentUnicast += v.MessageBucketsSentUnicast;
		MessageBucketsSentBroadcast += v.MessageBucketsSentBroadcast;
		MessagesReceivedWrongVertex += v.MessagesReceivedWrongVertex;
		MessagesReceivedCorrectVertex += v.MessagesReceivedCorrectVertex;
		DiscoveredNewVertexMachines += v.DiscoveredNewVertexMachines;

		ComputeTime += v.ComputeTime;
		StepFinishTime += v.StepFinishTime;
		IntersectCalcTime += v.IntersectCalcTime;
		UpdateVertexRegisters += v.UpdateVertexRegisters;
		RedirectedMessages += v.RedirectedMessages;
		MoveSendVertices += v.MoveSendVertices;
		MoveRecvVertices += v.MoveRecvVertices;
		MoveSendVerticesTime += v.MoveSendVerticesTime;
		MoveRecvVerticesTime += v.MoveRecvVerticesTime;

		MoveSendVerticesMessages += v.MoveSendVerticesMessages;
	}


	public static int getBytesLength() {
		return 18 * 8;
	}

	public String getString() {
		return MessagesTransmittedLocal
				+ ":" + MessagesSentUnicast
				+ ":" + MessagesSentBroadcast
				+ ":" + MessageBucketsSentUnicast
				+ ":" + MessageBucketsSentBroadcast
				+ ":" + MessagesReceivedWrongVertex
				+ ":" + MessagesReceivedCorrectVertex
				+ ":" + DiscoveredNewVertexMachines;
	}


	public long getWorkersTime() {
		return ComputeTime + IntersectCalcTime + StepFinishTime + MoveSendVerticesTime + MoveRecvVerticesTime;
	}
}