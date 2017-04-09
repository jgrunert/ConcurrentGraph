package mthesis.concurrent_graph.apps.shortestpath;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class SPMessageWritable extends BaseWritable {

	public int SrcVertex;
	public double Dist;
	public int DstVertex;//For testing
	public int SuperstepNo;//For testing


	public SPMessageWritable() {
		super();
	}

	public SPMessageWritable(int srcVertex, double dist, int dstVertex, int superstepNo) {
		super();
		SrcVertex = srcVertex;
		Dist = dist;
		DstVertex = dstVertex;
		SuperstepNo = superstepNo;
	}

	public SPMessageWritable setup(int srcVertex, double dist, int dstVertex, int superstepNo) {
		SrcVertex = srcVertex;
		Dist = dist;
		DstVertex = dstVertex;
		SuperstepNo = superstepNo;
		return this;
	}

	@Override
	public void readFromBuffer(ByteBuffer buffer) {
		SrcVertex = buffer.getInt();
		Dist = buffer.getDouble();
		DstVertex = buffer.getInt();
		SuperstepNo = buffer.getInt();
		int checkHash = buffer.getInt();
		if (checkHash != getCheckHash())
			logger.warn("Wrong check hash for message " + this);
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(SrcVertex);
		buffer.putDouble(Dist);
		buffer.putInt(DstVertex);
		buffer.putInt(SuperstepNo);
		buffer.putInt(getCheckHash());
	}

	// TODO Testcode
	private int getCheckHash() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(Dist);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + DstVertex;
		result = prime * result + SrcVertex;
		result = prime * result + SuperstepNo;
		return result;
	}

	@Override
	public String getString() {
		return SrcVertex + ":" + Dist + ":" + DstVertex + ":" + SuperstepNo;
	}

	@Override
	public int getBytesLength() {
		return 4 + 8 + 4 + 4;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<SPMessageWritable> {

		@Override
		public SPMessageWritable createDefault() {
			return new SPMessageWritable();
		}

		@Override
		public SPMessageWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SPMessageWritable(Integer.parseInt(sSplit[0]), Double.parseDouble(sSplit[1]),
					Integer.parseInt(sSplit[2]), Integer.parseInt(sSplit[3]));
		}
	}
}
