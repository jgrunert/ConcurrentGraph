package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class SSSPQueryValues extends BaseQueryGlobalValues {

	public int From;
	public int To;
	// Maximum distance. Initially set to infinite, set to target dist as soon as target discovered
	public double MaxDist;
	public boolean TargetFound; // TODO Remove?


	/**
	 * Public query creation constructor
	 */
	public SSSPQueryValues(int queryId, int from, int to) {
		this(queryId, 0, 0, from, to, Double.POSITIVE_INFINITY, false);
	}

	private SSSPQueryValues(int queryId, int activeVertices, int vertexCount, int from, int to, double maxDist, boolean targetFound) {
		super(queryId, activeVertices, vertexCount);
		From = from;
		To = to;
		MaxDist = maxDist;
		TargetFound = targetFound;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		super.writeToBuffer(buffer);
		buffer.putInt(From);
		buffer.putInt(To);
		buffer.putDouble(MaxDist);
		buffer.put(TargetFound ? (byte) 0 : (byte) 1);
	}

	@Override
	public String getString() {
		return super.getString() + ":" + From + ":" + To + ":" + MaxDist + ":" + TargetFound;
	}

	@Override
	public int getBytesLength() {
		return super.getBytesLength() + 2 * 4 + 8 + 1;
	}

	@Override
	public void combine(BaseQueryGlobalValues v) {
		SSSPQueryValues other = (SSSPQueryValues) v;
		MaxDist = Math.min(MaxDist, other.MaxDist);
		TargetFound |= (other).TargetFound;
		super.combine(v);
	}



	public static class Factory extends BaseQueryGlobalValuesFactory<SSSPQueryValues> {

		@Override
		public SSSPQueryValues createDefault(int queryId) {
			return new SSSPQueryValues(queryId, 0, 0, 0, 0, 0, false);
		}

		@Override
		public SSSPQueryValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPQueryValues(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]), Integer.parseInt(sSplit[2]),
					Integer.parseInt(sSplit[3]), Integer.parseInt(sSplit[4]), Double.parseDouble(sSplit[5]),
					Boolean.parseBoolean(sSplit[6]));
		}

		@Override
		public SSSPQueryValues createFromBytes(ByteBuffer bytes) {
			return new SSSPQueryValues(bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(),
					bytes.getDouble(), bytes.get() == 0);
		}
	}
}
