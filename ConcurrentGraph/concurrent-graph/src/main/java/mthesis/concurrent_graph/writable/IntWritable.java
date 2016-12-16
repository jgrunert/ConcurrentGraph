package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

public class IntWritable extends BaseWritable {

	public int Value;

	public IntWritable(int value) {
		super();
		Value = value;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putInt(Value);
	}

	@Override
	public String getString() {
		return Integer.toString(Value);
	}


	public static class Factory extends BaseWritableFactory<IntWritable> {
		@Override
		public IntWritable createFromString(String str) {
			return new IntWritable(Integer.parseInt(str));
		}

		@Override
		public IntWritable createFromBytes(ByteBuffer bytes) {
			return new IntWritable(bytes.getInt());
		}

	}
}
