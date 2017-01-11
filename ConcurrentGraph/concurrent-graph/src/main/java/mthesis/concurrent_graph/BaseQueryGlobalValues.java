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

	protected int ActiveVertices;
	protected int VertexCount;


	public BaseQueryGlobalValues(int activeVertices, int vertexCount) {
		super();
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

	public void add(BaseQueryGlobalValues v) {
		ActiveVertices += v.ActiveVertices;
		VertexCount += v.VertexCount;
	}

	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(ActiveVertices);
		buffer.putInt(VertexCount);
	}

	@Override
	public String getString() {
		return ActiveVertices + ":" + VertexCount;
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

		public abstract T createDefault();
	}

	public static class Factory extends BaseQueryGlobalValuesFactory<BaseQueryGlobalValues> {

		@Override
		public BaseQueryGlobalValues createDefault() {
			return new BaseQueryGlobalValues(0, 0);
		}

		@Override
		public BaseQueryGlobalValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new BaseQueryGlobalValues(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]));
		}

		@Override
		public BaseQueryGlobalValues createFromBytes(ByteBuffer bytes) {
			return new BaseQueryGlobalValues(bytes.getInt(), bytes.getInt());
		}
	}

	@Override
	public int getBytesLength() {
		return 2 * 4;
	}
}
