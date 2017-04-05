package mthesis.concurrent_graph.apps.shortestpath;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class SPMessageWritable extends BaseWritable {

	public int SrcVertex;
	public double Dist;


	public SPMessageWritable() {
		super();
	}

	public SPMessageWritable(int srcVertex, double dist) {
		super();
		SrcVertex = srcVertex;
		Dist = dist;
	}

	public SPMessageWritable setup(int srcVertex, double dist) {
		SrcVertex = srcVertex;
		Dist = dist;
		return this;
	}

	@Override
	public void readFromBuffer(ByteBuffer buffer) {
		SrcVertex = buffer.getInt();
		Dist = buffer.getDouble();
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


	public static class Factory extends BaseWritable.BaseWritableFactory<SPMessageWritable> {

		@Override
		public SPMessageWritable createDefault() {
			return new SPMessageWritable();
		}

		@Override
		public SPMessageWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SPMessageWritable(Integer.parseInt(sSplit[0]), Double.parseDouble(sSplit[1]));
		}
	}
}
