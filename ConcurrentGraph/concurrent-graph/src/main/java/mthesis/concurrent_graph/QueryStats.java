package mthesis.concurrent_graph;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class QueryStats {

	public static final Integer ComputeTimeKey = 0;
	public static final Integer StepFinishTimeKey = 1;
	public static final Integer IntersectCalcTimeKey = 2;
	public static final Integer UpdateVertexRegistersKey = 3;
	public static final Integer RedirectedMessagesKey = 4;
	public static final Integer MoveSendVerticsKey = 5;
	public static final Integer MoveRecvVerticsKey = 6;
	public static final Integer MoveSendVerticsTimeKey = 7;
	public static final Integer MoveRecvVerticsTimeKey = 8;

	public static final Map<Integer, String> OtherStatsNames;
	static {
		OtherStatsNames = new HashMap<Integer, String>();
		OtherStatsNames.put(ComputeTimeKey, "ComputeTime");
		OtherStatsNames.put(StepFinishTimeKey, "StepFinishTime");
		OtherStatsNames.put(IntersectCalcTimeKey, "IntersectCalcTime");
		OtherStatsNames.put(UpdateVertexRegistersKey, "UpdateVertexRegisters");
		OtherStatsNames.put(RedirectedMessagesKey, "RedirectedMessages");
		OtherStatsNames.put(MoveSendVerticsKey, "MoveSendVertics");
		OtherStatsNames.put(MoveRecvVerticsKey, "MoveRecvVertics");
		OtherStatsNames.put(MoveSendVerticsTimeKey, "MoveSendVerticsTime");
		OtherStatsNames.put(MoveRecvVerticsTimeKey, "MoveRecvVerticsTime");
	}

	// Direct variables for quick access of frequently changed variables
	public long MessagesTransmittedLocal;
	public long MessagesSentUnicast;
	public long MessagesSentBroadcast;
	public long MessageBucketsSentUnicast;
	public long MessageBucketsSentBroadcast;
	public long MessagesReceivedWrongVertex;
	public long MessagesReceivedCorrectVertex;
	public long DiscoveredNewVertexMachines;

	// Other stats which are less frequently changed
	public final Map<Integer, Long> OtherStats;


	public QueryStats() {
		OtherStats = new HashMap<>();
	}

	public QueryStats(long messagesTransmittedLocal, long messagesSentUnicast, long messagesSentBroadcast,
			long messageBucketsSentUnicast,
			long messageBucketsSentBroadcast, long messagesReceivedWrongVertex, long messagesReceivedCorrectVertex,
			long discoveredNewVertexMachines,
			Map<Integer, Long> otherStats) {
		super();
		MessagesTransmittedLocal = messagesTransmittedLocal;
		MessagesSentUnicast = messagesSentUnicast;
		MessagesSentBroadcast = messagesSentBroadcast;
		MessageBucketsSentUnicast = messageBucketsSentUnicast;
		MessageBucketsSentBroadcast = messageBucketsSentBroadcast;
		MessagesReceivedWrongVertex = messagesReceivedWrongVertex;
		MessagesReceivedCorrectVertex = messagesReceivedCorrectVertex;
		DiscoveredNewVertexMachines = discoveredNewVertexMachines;
		OtherStats = otherStats;
	}

	public QueryStats(ByteBuffer bytes) {
		super();
		MessagesTransmittedLocal = bytes.getLong();
		MessagesSentUnicast = bytes.getLong();
		MessagesSentBroadcast = bytes.getLong();
		MessageBucketsSentUnicast = bytes.getLong();
		MessageBucketsSentBroadcast = bytes.getLong();
		MessagesReceivedWrongVertex = bytes.getLong();
		MessagesReceivedCorrectVertex = bytes.getLong();
		DiscoveredNewVertexMachines = bytes.getLong();

		int numOtherStats = bytes.getInt();
		OtherStats = new HashMap<>(numOtherStats);
		for (int i = 0; i < numOtherStats; i++) {
			OtherStats.put(bytes.getInt(), bytes.getLong());
		}
	}


	public void setOtherStat(int key, long value) {
		OtherStats.put(key, value);
	}

	public void addToOtherStat(int key, long toAdd) {
		Long thisStat = OtherStats.get(key);
		if (thisStat == null) thisStat = 0L;
		OtherStats.put(key, thisStat + toAdd);
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

		for (Entry<Integer, Long> stat : v.OtherStats.entrySet()) {
			Long thisStat = OtherStats.get(stat.getKey());
			if (thisStat == null) thisStat = 0L;
			OtherStats.put(stat.getKey(), thisStat + stat.getValue());
		}
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

		buffer.putInt(OtherStats.size());
		for (Entry<Integer, Long> stat : OtherStats.entrySet()) {
			buffer.putInt(stat.getKey());
			buffer.putLong(stat.getValue());
		}
	}

	public int getBytesLength() {
		return 11 * 8 + 4 + 12 * OtherStats.size();
	}

	public String getString() {
		return MessagesTransmittedLocal
				+ ":" + MessagesSentUnicast
				+ ":" + MessagesSentBroadcast
				+ ":" + MessageBucketsSentUnicast
				+ ":" + MessageBucketsSentBroadcast
				+ ":" + MessagesReceivedWrongVertex
				+ ":" + MessagesReceivedCorrectVertex
				+ ":" + DiscoveredNewVertexMachines
				+ ":" + getOtherStatsString();
	}

	public String getOtherStatsString() {
		StringBuilder sb = new StringBuilder();
		for (Entry<Integer, Long> stat : OtherStats.entrySet()) {
			sb.append(OtherStatsNames.get(stat.getKey()));
			sb.append(':');
			sb.append(stat.getValue());
			sb.append(", ");
		}
		return sb.toString();
	}
}