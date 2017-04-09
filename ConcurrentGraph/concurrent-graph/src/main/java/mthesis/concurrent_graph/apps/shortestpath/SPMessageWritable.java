package mthesis.concurrent_graph.apps.shortestpath;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class SPMessageWritable extends BaseWritable {

	public int SrcVertex;
	public double Dist;
	public int SuperstepNo;//For testing


	public SPMessageWritable() {
		super();
	}

	public SPMessageWritable(int srcVertex, double dist, int superstepNo) {
		super();
		SrcVertex = srcVertex;
		Dist = dist;
		SuperstepNo = superstepNo;
	}

	public SPMessageWritable setup(int srcVertex, double dist, int superstepNo) {
		SrcVertex = srcVertex;
		Dist = dist;
		SuperstepNo = superstepNo;
		return this;
	}

	@Override
	public void readFromBuffer(ByteBuffer buffer) {
		SrcVertex = buffer.getInt();
		Dist = buffer.getDouble();
		SuperstepNo = buffer.getInt();
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(SrcVertex);
		buffer.putDouble(Dist);
		buffer.putInt(SuperstepNo);
	}

	@Override
	public String getString() {
		return SrcVertex + ":" + Dist + ":" + SuperstepNo;
	}

	@Override
	public int getBytesLength() {
		return 4 + 8 + 4;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<SPMessageWritable> {

		@Override
		public SPMessageWritable createDefault() {
			return new SPMessageWritable();
		}

		@Override
		public SPMessageWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SPMessageWritable(Integer.parseInt(sSplit[0]), Double.parseDouble(sSplit[1]), Integer.parseInt(sSplit[2]));
		}
	}
}
