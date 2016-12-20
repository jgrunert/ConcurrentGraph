package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class SSSPVertexWritable extends BaseWritable {

	public int Pre;
	public double Dist;


	public SSSPVertexWritable(int pre, double dist) {
		super();
		Pre = pre;
		Dist = dist;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(Pre);
		buffer.putDouble(Dist);
	}

	@Override
	public String getString() {
		return Pre + ":" + Dist;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<SSSPVertexWritable> {

		@Override
		public SSSPVertexWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new SSSPVertexWritable(Integer.parseInt(sSplit[0]), Double.parseDouble(sSplit[1]));
		}

		@Override
		public SSSPVertexWritable createFromBytes(ByteBuffer bytes) {
			return new SSSPVertexWritable(bytes.getInt(), bytes.getDouble());
		}
	}
}
