package mthesis.concurrent_graph.examples.cc;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.writable.BaseWritable;

public class CCMessageWritable extends BaseWritable {

	public int SrcVertex;
	public int Value;


	public CCMessageWritable() {
		super();
	}

	public CCMessageWritable(int srcVertex, int value) {
		super();
		SrcVertex = srcVertex;
		Value = value;
	}


	@Override
	public void readFromBuffer(ByteBuffer buffer) {
		SrcVertex = buffer.getInt();
		Value = buffer.getInt();
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(SrcVertex);
		buffer.putInt(Value);
	}

	@Override
	public String getString() {
		return SrcVertex + ":" + Value;
	}

	@Override
	public int getBytesLength() {
		return 2 * 4;
	}


	public static class Factory extends BaseWritable.BaseWritableFactory<CCMessageWritable> {

		@Override
		public CCMessageWritable createDefault() {
			return new CCMessageWritable();
		}

		@Override
		public CCMessageWritable createFromString(String str) {
			final String[] sSplit = str.split(":");
			return new CCMessageWritable(Integer.parseInt(sSplit[0]), Integer.parseInt(sSplit[1]));
		}
	}
}
