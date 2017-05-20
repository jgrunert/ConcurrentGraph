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
	public long SuperstepsComputed;
	public long LocalSuperstepsComputed;
	public long LocalmodeStops;



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
		SuperstepsComputed = bytes.getLong();
		LocalSuperstepsComputed = bytes.getLong();
		LocalmodeStops = bytes.getLong();
	}


	public void writeToStream(DataOutputStream stream) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(getBytesLength());
		writeToBuffer(buffer);
		stream.write(buffer.array(), 0, buffer.position());
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
		buffer.putLong(SuperstepsComputed);
		buffer.putLong(LocalSuperstepsComputed);
		buffer.putLong(LocalmodeStops);
	}

	public Map<String, Double> getStatsMap(int numWorkers) {
		Map<String, Double> statsMap = new TreeMap<>();

		statsMap.put("MessagesTransmittedLocal", (double) MessagesTransmittedLocal);
		statsMap.put("MessagesSentUnicast", (double) MessagesSentUnicast);
		statsMap.put("MessagesSentBroadcast", (double) MessagesSentBroadcast);
		statsMap.put("MessagesSentTotal", (double) MessagesSentUnicast + MessagesSentBroadcast);
		statsMap.put("MessageBucketsSentUnicast", (double) MessageBucketsSentUnicast);
		statsMap.put("MessageBucketsSentBroadcast", (double) MessageBucketsSentBroadcast);
		statsMap.put("MessageBucketsSentTotal", (double) MessageBucketsSentUnicast + MessageBucketsSentBroadcast);
		statsMap.put("MessagesReceivedWrongVertex", (double) MessagesReceivedWrongVertex);
		statsMap.put("MessagesReceivedCorrectVertex", (double) MessagesReceivedCorrectVertex);
		statsMap.put("DiscoveredNewVertexMachines", (double) DiscoveredNewVertexMachines);

		statsMap.put("ComputeTime", (double) ComputeTime);
		statsMap.put("StepFinishTime", (double) StepFinishTime);
		statsMap.put("SuperstepsComputed", (double) SuperstepsComputed);
		statsMap.put("LocalSuperstepsComputed", (double) LocalSuperstepsComputed);
		if (SuperstepsComputed > 0) {
			long totalSupersteps = SuperstepsComputed;
			long localSupersteps = LocalSuperstepsComputed;
			double nonlocalSuperstepsUnique = (double) (totalSupersteps - localSupersteps) / numWorkers;
			double totalSuperstepsUnique = localSupersteps + nonlocalSuperstepsUnique;
			statsMap.put("LocalSuperstepsRatio", (double) LocalSuperstepsComputed * 100 / SuperstepsComputed);
			statsMap.put("SuperstepsComputedUnique", totalSuperstepsUnique);
			statsMap.put("LocalSuperstepsRatioUnique", localSupersteps * 100 / totalSuperstepsUnique);
		}
		else {
			statsMap.put("LocalSuperstepsRatio", (double) 0);
			statsMap.put("SuperstepsComputedUnique", 0D);
			statsMap.put("LocalSuperstepsRatioUnique", 0D);
		}
		statsMap.put("LocalmodeStops", (double) LocalmodeStops);

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
		SuperstepsComputed += v.SuperstepsComputed;
		LocalSuperstepsComputed += v.LocalSuperstepsComputed;
		LocalmodeStops += v.LocalmodeStops;
	}


	public static int getBytesLength() {
		return 13 * 8;
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


	public long getTimeSum() {
		return ComputeTime + StepFinishTime;
	}
}