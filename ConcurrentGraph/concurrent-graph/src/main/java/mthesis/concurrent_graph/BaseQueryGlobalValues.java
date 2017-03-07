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

	public int QueryId;
	protected int ActiveVertices;
	protected int VertexCount;
	public QueryStats Stats;


	public BaseQueryGlobalValues() {
		super();
	}

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
	public void readFromBuffer(ByteBuffer buffer) {
		QueryId = buffer.getInt();
		ActiveVertices = buffer.getInt();
		VertexCount = buffer.getInt();
		Stats = new QueryStats(buffer);
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

	public int GetQueryHash() {
		return 0;
	}



	public static abstract class BaseQueryGlobalValuesFactory<T extends BaseQueryGlobalValues> extends BaseWritableFactory<T> {

		public abstract T createDefault(int queryId);
	}

	public static class Factory extends BaseQueryGlobalValuesFactory<BaseQueryGlobalValues> {

		@Override
		public BaseQueryGlobalValues createDefault() {
			return new BaseQueryGlobalValues();
		}

		@Override
		public BaseQueryGlobalValues createDefault(int queryId) {
			return new BaseQueryGlobalValues(queryId);
		}

		@Override
		public BaseQueryGlobalValues createFromString(String str) {
			throw new RuntimeException("createFromString not implemented for BaseQueryGlobalValues");
		}
	}
}
