package mthesis.concurrent_graph.examples.cc;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class CCMessageWritable extends BaseWritable {

	public CCMessageWritable(int dstVertex, int value) {
		super();
		SrcVertex = dstVertex;
		Value = value;
	}

	public int SrcVertex;
	public int Value;


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(SrcVertex);
		buffer.putInt(Value);
	}

	@Override
	public String getString() {
		return SrcVertex + ":" + Value;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<CCMessageWritable> {

		@Override
		public CCMessageWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new CCMessageWritable(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]));
		}

		@Override
		public CCMessageWritable createFromBytes(ByteBuffer bytes) {
			return new CCMessageWritable(bytes.getInt(), bytes.getInt());
		}
	}
}
