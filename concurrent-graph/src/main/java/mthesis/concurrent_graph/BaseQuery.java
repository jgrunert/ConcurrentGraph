package mthesis.concurrent_graph;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Base class for query global values and control, such as initial configuration or aggregators.
 * Configuration values remain unchanged while aggregated values are aggregated by the master.
 *
 * @author Jonas Grunert
 *
 */
public class BaseQuery extends BaseWritable {

	public int QueryId;
	protected int ActiveVertices;
	protected int VertexCount;
	public QueryStats Stats;


	public BaseQuery() {
		super();
	}

	public BaseQuery(int queryId) {
		super();
		QueryId = queryId;
		ActiveVertices = 0;
		VertexCount = 0;
		Stats = new QueryStats();
	}

	public BaseQuery(int queryId, int activeVertices, int vertexCount, QueryStats stats) {
		super();
		QueryId = queryId;
		ActiveVertices = activeVertices;
		VertexCount = vertexCount;
		Stats = stats;
	}

	public void combine(BaseQuery v) {
		if (QueryId != v.QueryId) throw new RuntimeException("Cannot add qureries with differend IDs: " + QueryId + " " + v.QueryId);
		ActiveVertices += v.ActiveVertices;
		VertexCount += v.VertexCount;
		Stats.combine(v.Stats);
	}


	/**
	 * Called by master when no more vertices are active
	 * @return TRUE if the query is finished now
	 */
	public boolean onMasterAllVerticesFinished() {
		return true;
	}

	/**
	 * Called by master if all workers must be forced active in the next superstep.
	 * @return TRUE if force all workers to be active.
	 */
	public boolean masterForceAllWorkersActive(int superstepNo) {
		return superstepNo <= 0;
	}

	/**
	 * Called by worker before the computation of a new superstep is started
	 * @return TRUE if all vertices should be activated this superstep.
	 */
	public boolean onWorkerSuperstepStart(int superstepNo) {
		return superstepNo == 0;
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
		return 3 * 4 + QueryStats.getBytesLength();
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



	public static abstract class BaseQueryGlobalValuesFactory<T extends BaseQuery> extends BaseWritableFactory<T> {

		public abstract T createDefault(int queryId);
	}

	public static class Factory extends BaseQueryGlobalValuesFactory<BaseQuery> {

		@Override
		public BaseQuery createDefault() {
			return new BaseQuery();
		}

		@Override
		public BaseQuery createDefault(int queryId) {
			return new BaseQuery(queryId);
		}

		@Override
		public BaseQuery createFromString(String str) {
			throw new RuntimeException("createFromString not implemented for BaseQueryGlobalValues");
		}
	}
}
