package mthesis.concurrent_graph;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Base class for query global values such as initial configuration or aggregators.
 * Configuration values remain unchanged while aggregated values are aggregated by the master.
 *
 * @author Jonas Grunert
 *
 */
public class BaseQueryGlobalValues extends BaseWritable {

	public final int QueryId;
	protected int ActiveVertices;
	protected int VertexCount;
	public final QueryStats Stats;


	public BaseQueryGlobalValues(int queryId) {
		super();
		QueryId = queryId;
		ActiveVertices = 0;
		VertexCount = 0;
		Stats = new QueryStats();
	}

	public BaseQueryGlobalValues(int queryId, int activeVertices, int vertexCount, QueryStats stats) {
		super();
		QueryId = queryId;
		ActiveVertices = activeVertices;
		VertexCount = vertexCount;
		Stats = stats;
	}

	public void combine(BaseQueryGlobalValues v) {
		if (QueryId != v.QueryId) throw new RuntimeException("Cannot add qureries with differend IDs: " + QueryId + " " + v.QueryId);
		ActiveVertices += v.ActiveVertices;
		VertexCount += v.VertexCount;
		Stats.combine(v.Stats);
	}

	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(QueryId);
		buffer.putInt(ActiveVertices);
		buffer.putInt(VertexCount);
		Stats.writeToBuffer(buffer);
	}

	@Override
	public String getString() {
		return QueryId + ":" + ActiveVertices + ":" + VertexCount
				+ ":" + Stats.getString();
	}


	@Override
	public int getBytesLength() {
		return 3 * 4 + Stats.getBytesLength();
	}



	public int getActiveVertices() {
		return ActiveVertices;
	}


	public void setActiveVertices(int activeVertices) {
		ActiveVertices = activeVertices;
	}


	public int getVertexCount() {
		return VertexCount;
	}


	public void setVertexCount(int vertexCount) {
		VertexCount = vertexCount;
	}



	public static abstract class BaseQueryGlobalValuesFactory<T extends BaseQueryGlobalValues> extends BaseWritableFactory<T> {

		public abstract T createDefault(int queryId);
	}

	public static class Factory extends BaseQueryGlobalValuesFactory<BaseQueryGlobalValues> {

		@Override
		public BaseQueryGlobalValues createDefault(int queryId) {
			return new BaseQueryGlobalValues(queryId);
		}

		@Override
		public BaseQueryGlobalValues createFromString(String str) {
			throw new RuntimeException("createFromString not implemented for BaseQueryGlobalValues");
		}

		@Override
		public BaseQueryGlobalValues createFromBytes(ByteBuffer bytes) {
			return new BaseQueryGlobalValues(bytes.getInt(), bytes.getInt(), bytes.getInt(),
					new QueryStats(bytes));
		}
	}


	public static class QueryStats {

		public int MessagesTransmittedLocal;
		public int MessagesSentUnicast;
		public int MessagesSentBroadcast;
		public int MessageBucketsSentUnicast;
		public int MessageBucketsSentBroadcast;
		public int MessagesReceivedWrongVertex;
		public int MessagesReceivedCorrectVertex;
		public int DiscoveredNewVertexMachines;
		public int ComputeTime;
		public int StepFinishTime;
		public int IntersectCalcTime;

		public QueryStats() {
		}

		public QueryStats(int messagesTransmittedLocal, int messagesSentUnicast, int messagesSentBroadcast, int messageBucketsSentUnicast,
				int messageBucketsSentBroadcast, int messagesReceivedWrongVertex, int messagesReceivedCorrectVertex,
				int discoveredNewVertexMachines,
				int computeTime, int barrierTime, int intersectCalcTime) {
			super();
			MessagesTransmittedLocal = messagesTransmittedLocal;
			MessagesSentUnicast = messagesSentUnicast;
			MessagesSentBroadcast = messagesSentBroadcast;
			MessageBucketsSentUnicast = messageBucketsSentUnicast;
			MessageBucketsSentBroadcast = messageBucketsSentBroadcast;
			MessagesReceivedWrongVertex = messagesReceivedWrongVertex;
			MessagesReceivedCorrectVertex = messagesReceivedCorrectVertex;
			DiscoveredNewVertexMachines = discoveredNewVertexMachines;
			ComputeTime = computeTime;
			StepFinishTime = barrierTime;
			IntersectCalcTime = intersectCalcTime;
		}

		public QueryStats(ByteBuffer bytes) {
			super();
			MessagesTransmittedLocal = bytes.getInt();
			MessagesSentUnicast = bytes.getInt();
			MessagesSentBroadcast = bytes.getInt();
			MessageBucketsSentUnicast = bytes.getInt();
			MessageBucketsSentBroadcast = bytes.getInt();
			MessagesReceivedWrongVertex = bytes.getInt();
			MessagesReceivedCorrectVertex = bytes.getInt();
			DiscoveredNewVertexMachines = bytes.getInt();
			ComputeTime = bytes.getInt();
			StepFinishTime = bytes.getInt();
			IntersectCalcTime = bytes.getInt();
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
		}


		public void writeToBuffer(ByteBuffer buffer) {
			buffer.putInt(MessagesTransmittedLocal);
			buffer.putInt(MessagesSentUnicast);
			buffer.putInt(MessagesSentBroadcast);
			buffer.putInt(MessageBucketsSentUnicast);
			buffer.putInt(MessageBucketsSentBroadcast);
			buffer.putInt(MessagesReceivedWrongVertex);
			buffer.putInt(MessagesReceivedCorrectVertex);
			buffer.putInt(DiscoveredNewVertexMachines);
			buffer.putInt(ComputeTime);
			buffer.putInt(StepFinishTime);
			buffer.putInt(IntersectCalcTime);
		}

		public int getBytesLength() {
			return 11 * 4;
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
					+ ":" + ComputeTime
					+ ":" + StepFinishTime
					+ ":" + IntersectCalcTime;
		}
	}
}
