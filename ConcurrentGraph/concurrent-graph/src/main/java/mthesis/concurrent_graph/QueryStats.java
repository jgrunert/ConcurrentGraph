package mthesis.concurrent_graph;

import java.io.DataOutputStream;
import java.io.IOException;
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
	public long UpdateVertexRegisters;
	public long RedirectedMessages;
	public long MoveSendVertices;
	public long MoveRecvVertices;
	public long MoveSendVerticesTime;
	public long MoveRecvVerticesTime;

	// Detailed stats
	public long MoveSendVerticesMessages;



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
		UpdateVertexRegisters = bytes.getLong();
		RedirectedMessages = bytes.getLong();
		MoveSendVertices = bytes.getLong();
		MoveRecvVertices = bytes.getLong();
		MoveSendVerticesTime = bytes.getLong();
		MoveRecvVerticesTime = bytes.getLong();

		MoveSendVerticesMessages = bytes.getLong();
	}


	public void writeToStream(DataOutputStream stream) throws IOException {
		stream.writeLong(MessagesTransmittedLocal);
		stream.writeLong(MessagesSentUnicast);
		stream.writeLong(MessagesSentBroadcast);
		stream.writeLong(MessageBucketsSentUnicast);
		stream.writeLong(MessageBucketsSentBroadcast);
		stream.writeLong(MessagesReceivedWrongVertex);
		stream.writeLong(MessagesReceivedCorrectVertex);
		stream.writeLong(DiscoveredNewVertexMachines);

		stream.writeLong(ComputeTime);
		stream.writeLong(StepFinishTime);
		stream.writeLong(UpdateVertexRegisters);
		stream.writeLong(RedirectedMessages);
		stream.writeLong(MoveSendVertices);
		stream.writeLong(MoveRecvVertices);
		stream.writeLong(MoveSendVerticesTime);
		stream.writeLong(MoveRecvVerticesTime);

		stream.writeLong(MoveSendVerticesMessages);
	}


	public Map<String, Double> getStatsMap() {
		Map<String, Double> statsMap = new TreeMap<>();

		statsMap.put("MessagesTransmittedLocal", (double) MessagesTransmittedLocal);
		statsMap.put("MessagesSentUnicast", (double) MessagesSentUnicast);
		statsMap.put("MessagesSentBroadcast", (double) MessagesSentBroadcast);
		statsMap.put("MessageBucketsSentUnicast", (double) MessageBucketsSentUnicast);
		statsMap.put("MessageBucketsSentBroadcast", (double) MessageBucketsSentBroadcast);
		statsMap.put("MessagesReceivedWrongVertex", (double) MessagesReceivedWrongVertex);
		statsMap.put("MessagesReceivedCorrectVertex", (double) MessagesReceivedCorrectVertex);
		statsMap.put("DiscoveredNewVertexMachines", (double) DiscoveredNewVertexMachines);

		statsMap.put("ComputeTime", (double) ComputeTime);
		statsMap.put("StepFinishTime", (double) StepFinishTime);
		statsMap.put("UpdateVertexRegisters", (double) UpdateVertexRegisters);
		statsMap.put("RedirectedMessages", (double) RedirectedMessages);
		statsMap.put("MoveSendVertices", (double) MoveSendVertices);
		statsMap.put("MoveRecvVertices", (double) MoveRecvVertices);
		statsMap.put("MoveSendVerticesTime", (double) MoveSendVerticesTime);
		statsMap.put("MoveRecvVerticesTime", (double) MoveRecvVerticesTime);

		statsMap.put("MoveSendVerticesMessages", (double) MoveSendVerticesMessages);

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
		return ComputeTime + StepFinishTime + MoveSendVerticesTime + MoveRecvVerticesTime;
	}
}