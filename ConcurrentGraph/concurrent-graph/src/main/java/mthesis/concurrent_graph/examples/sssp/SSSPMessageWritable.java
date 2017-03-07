package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class SSSPMessageWritable extends BaseWritable {

	public int SrcVertex;
	public double Dist;


	public SSSPMessageWritable() {
		super();
	}

	public SSSPMessageWritable(int srcVertex, double dist) {
		super();
		SrcVertex = srcVertex;
		Dist = dist;
	}

	public SSSPMessageWritable setup(int srcVertex, double dist) {
		SrcVertex = srcVertex;
		Dist = dist;
		return this;
	}

	@Override
	public void readFromBuffer(ByteBuffer buffer) {
		SrcVertex = buffer.getInt();
		Dist = buffer.getInt();
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(SrcVertex);
		buffer.putDouble(Dist);
	}

	@Override
	public String getString() {
		return SrcVertex + ":" + Dist;
	}

	@Override
	public int getBytesLength() {
		return 4 + 8;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<SSSPMessageWritable> {

		@Override
		public SSSPMessageWritable createDefault() {
			return new SSSPMessageWritable();
		}

		@Override
		public SSSPMessageWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPMessageWritable(Integer.parseInt(sSplit[0]), Double.parseDouble(sSplit[1]));
		}
	}
}
