package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class SSSPVertexWritable extends BaseWritable {

	public int Pre;
	public double Dist;
	// Indicates if a superstep was skipped because of distance limit and messages should be sent later.
	public boolean SendMsgsLater;


	public SSSPVertexWritable() {
		super();
	}

	public SSSPVertexWritable(int pre, double dist, boolean sendMsgsLater) {
		super();
		Pre = pre;
		Dist = dist;
		SendMsgsLater = sendMsgsLater;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(Pre);
		buffer.putDouble(Dist);
		buffer.put(SendMsgsLater ? (byte) 0 : (byte) 1);
	}

	@Override
	public String getString() {
		return Pre + ":" + Dist + ":" + SendMsgsLater;
	}

	@Override
	public int getBytesLength() {
		return 4 + 8;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<SSSPVertexWritable> {

		@Override
		public SSSPVertexWritable createDefault() {
			return new SSSPVertexWritable();
		}

		@Override
		public SSSPVertexWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPVertexWritable(Integer.parseInt(sSplit[0]), Double.parseDouble(sSplit[1]), Boolean.parseBoolean(sSplit[2]));
		}

		@Override
		public SSSPVertexWritable createFromBytes(ByteBuffer bytes) {
			return new SSSPVertexWritable(bytes.getInt(), bytes.getDouble(), bytes.get() == 0);
		}

		@Override
		public SSSPVertexWritable createClone(SSSPVertexWritable toClone) {
			return new SSSPVertexWritable(toClone.Pre, toClone.Dist, toClone.SendMsgsLater);
		}
	}
}
