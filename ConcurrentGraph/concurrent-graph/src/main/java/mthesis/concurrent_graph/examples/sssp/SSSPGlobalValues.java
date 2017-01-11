package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class SSSPGlobalValues extends BaseQueryGlobalValues {

	public int From;
	public int To;
	public double MaxDist;


	public SSSPGlobalValues(int from, int to, double maxDist) {
		this(0, 0, from, to, maxDist);
	}

	private SSSPGlobalValues(int activeVertices, int vertexCount, int from, int to, double maxDist) {
		super(activeVertices, vertexCount);
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
		public SSSPGlobalValues createDefault() {
			return new SSSPGlobalValues(0, 0, 0, 0, 0);
		}

		@Override
		public SSSPGlobalValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPGlobalValues(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]), Integer.parseInt(sSplit[2]),
					Integer.parseInt(sSplit[3]), Double.parseDouble(sSplit[4]));
		}

		@Override
		public SSSPGlobalValues createFromBytes(ByteBuffer bytes) {
			return new SSSPGlobalValues(bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getDouble());
		}
	}
}
