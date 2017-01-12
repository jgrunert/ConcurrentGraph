package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class SSSPQueryValues extends BaseQueryGlobalValues {

	public int From;
	public int To;
	public double MaxDist;


	public SSSPQueryValues(int queryId, int from, int to, double maxDist) {
		this(queryId, 0, 0, from, to, maxDist);
	}

	private SSSPQueryValues(int queryId, int activeVertices, int vertexCount, int from, int to, double maxDist) {
		super(queryId, activeVertices, vertexCount);
		From = from;
		To = to;
		MaxDist = maxDist;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		super.writeToBuffer(buffer);
		buffer.putInt(From);
		buffer.putInt(To);
		buffer.putDouble(MaxDist);
	}

	@Override
	public String getString() {
		return super.getString() + ":" + From + ":" + To + ":" + MaxDist;
	}

	@Override
	public int getBytesLength() {
		return super.getBytesLength() + 2 * 4 + 8;
	}



	public static class Factory extends BaseQueryGlobalValuesFactory<SSSPQueryValues> {

		@Override
		public SSSPQueryValues createDefault(int queryId) {
			return new SSSPQueryValues(queryId, 0, 0, 0, 0, 0);
		}

		@Override
		public SSSPQueryValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPQueryValues(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]), Integer.parseInt(sSplit[2]),
					Integer.parseInt(sSplit[3]), Integer.parseInt(sSplit[4]), Double.parseDouble(sSplit[5]));
		}

		@Override
		public SSSPQueryValues createFromBytes(ByteBuffer bytes) {
			return new SSSPQueryValues(bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getDouble());
		}
	}
}
