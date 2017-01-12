package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class SSSPGlobalValues extends BaseQueryGlobalValues {

	public int From;
	public int To;
	public double MaxDist;


	public SSSPGlobalValues(int queryId, int from, int to, double maxDist) {
		this(queryId, 0, 0, from, to, maxDist);
	}

	private SSSPGlobalValues(int queryId, int activeVertices, int vertexCount, int from, int to, double maxDist) {
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



	public static class Factory extends BaseQueryGlobalValuesFactory<SSSPGlobalValues> {

		@Override
		public SSSPGlobalValues createDefault(int queryId) {
			return new SSSPGlobalValues(queryId, 0, 0, 0, 0, 0);
		}

		@Override
		public SSSPGlobalValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPGlobalValues(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]), Integer.parseInt(sSplit[2]),
					Integer.parseInt(sSplit[3]), Integer.parseInt(sSplit[4]), Double.parseDouble(sSplit[5]));
		}

		@Override
		public SSSPGlobalValues createFromBytes(ByteBuffer bytes) {
			return new SSSPGlobalValues(bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getDouble());
		}
	}
}
