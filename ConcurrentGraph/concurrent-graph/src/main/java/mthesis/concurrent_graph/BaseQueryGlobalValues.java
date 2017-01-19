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


	public BaseQueryGlobalValues(int queryId, int activeVertices, int vertexCount) {
		super();
		QueryId = queryId;
		ActiveVertices = activeVertices;
		VertexCount = vertexCount;
	}

	//	public QueryGlobalValues aggregate(List<QueryGlobalValues> singleValues) {
	//		QueryGlobalValues aggregated = new QueryGlobalValues();
	//		for (QueryGlobalValues v : singleValues) {
	//			aggregated.ActiveVertices += v.ActiveVertices;
	//			aggregated.VertexCount += v.VertexCount;
	//		}
	//		return aggregated;
	//	}	

	public void combine(BaseQueryGlobalValues v) {
		if (QueryId != v.QueryId) throw new RuntimeException("Cannot add qureries with differend IDs: " + QueryId + " " + v.QueryId);
		ActiveVertices += v.ActiveVertices;
		VertexCount += v.VertexCount;
	}

	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(QueryId);
		buffer.putInt(ActiveVertices);
		buffer.putInt(VertexCount);
	}

	@Override
	public String getString() {
		return QueryId + ":" + ActiveVertices + ":" + VertexCount;
	}


	//	public static abstract class BaseWritableFactory<T>{
	//		public abstract T createFromString(String str);
	//		public abstract T createFromBytes(ByteBuffer bytes);
	//	}



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
			return new BaseQueryGlobalValues(queryId, 0, 0);
		}

		@Override
		public BaseQueryGlobalValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new BaseQueryGlobalValues(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]), Integer.parseInt(sSplit[2]));
		}

		@Override
		public BaseQueryGlobalValues createFromBytes(ByteBuffer bytes) {
			return new BaseQueryGlobalValues(bytes.getInt(), bytes.getInt(), bytes.getInt());
		}
	}

	@Override
	public int getBytesLength() {
		return 3 * 4;
	}
}
