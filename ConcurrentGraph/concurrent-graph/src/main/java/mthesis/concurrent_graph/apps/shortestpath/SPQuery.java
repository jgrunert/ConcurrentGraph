package mthesis.concurrent_graph.apps.shortestpath;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.QueryStats;


public class SPQuery extends BaseQuery {

	public int From;
	public int To;
	// Maximum distance. Initially set to infinite, set to target dist as soon as target discovered
	public double MaxDist;
	public boolean ReconstructionPhase;


	/**
	 * Public query creation constructor
	 */
	public SPQuery(int queryId, int from, int to) {
		super(queryId);
		From = from;
		To = to;
		MaxDist = Double.POSITIVE_INFINITY;
		ReconstructionPhase = false;
	}

	private SPQuery(int queryId, int activeVertices, int vertexCount, QueryStats stats,
			int from, int to, double maxDist, boolean targetFound) {
		super(queryId, activeVertices, vertexCount, stats);
		From = from;
		To = to;
		MaxDist = maxDist;
		ReconstructionPhase = targetFound;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		super.writeToBuffer(buffer);
		buffer.putInt(From);
		buffer.putInt(To);
		buffer.putDouble(MaxDist);
		buffer.put(ReconstructionPhase ? (byte) 0 : (byte) 1);
	}

	@Override
	public String getString() {
		return super.getString() + ":" + From + ":" + To + ":" + MaxDist + ":" + ReconstructionPhase;
	}

	@Override
	public int getBytesLength() {
		return super.getBytesLength() + 2 * 4 + 8 + 1;
	}

	@Override
	public int GetQueryHash() {
		final int prime = 31;
		int result = 1;
		result = prime * result + From;
		result = prime * result + To;
		return result;
	}

	@Override
	public void combine(BaseQuery v) {
		SPQuery other = (SPQuery) v;
		MaxDist = Math.min(MaxDist, other.MaxDist);
		ReconstructionPhase |= (other).ReconstructionPhase;
		super.combine(v);
	}



	public static class Factory extends BaseQueryGlobalValuesFactory<SPQuery> {

		@Override
		public SPQuery createDefault() {
			throw new RuntimeException("Not supported");
		}

		@Override
		public SPQuery createDefault(int queryId) {
			return new SPQuery(queryId, 0, 0);
		}

		@Override
		public SPQuery createFromString(String str) {
			throw new RuntimeException("createFromString not implemented for BaseQueryGlobalValues");
		}

		@Override
		public SPQuery createFromBytes(ByteBuffer bytes) {
			return new SPQuery(bytes.getInt(), bytes.getInt(), bytes.getInt(), new QueryStats(bytes),
					bytes.getInt(), bytes.getInt(), bytes.getDouble(), bytes.get() == 0);
		}
	}
}
