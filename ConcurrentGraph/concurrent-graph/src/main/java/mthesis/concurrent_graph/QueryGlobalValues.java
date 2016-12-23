package mthesis.concurrent_graph;

import java.nio.ByteBuffer;
import java.util.List;

import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Base class for query global values such as initial values or aggregators.
 * 
 * @author Jonas Grunert
 *
 */
public class QueryGlobalValues extends BaseWritable {

	protected int ActiveVertices;
	protected int VertexCount;


	public QueryGlobalValues aggregate(List<QueryGlobalValues> singleValues) {
		QueryGlobalValues aggregated = new QueryGlobalValues();
		for (QueryGlobalValues v : singleValues) {
			aggregated.ActiveVertices += v.ActiveVertices;
			aggregated.VertexCount += v.VertexCount;
		}
		return aggregated;
	}

	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getString() {
		return null;
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
}
