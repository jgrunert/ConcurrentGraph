package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

public class IntWritable extends BaseWritable {

	public int Value;

	public IntWritable(int value) {
		super();
		Value = value;
	}


	@Override
	public ByteBuffer GetBytes() {
		final ByteBuffer bytes = ByteBuffer.allocate(4);
		bytes.putInt(Value);
		return bytes;
	}

	@Override
	public String GetString() {
		return Integer.toString(Value);
	}


	public static class Factory extends BaseWritableFactory<IntWritable> {
		@Override
		public IntWritable CreateFromString(String str) {
			return new IntWritable(Integer.parseInt(str));
		}

		@Override
		public IntWritable CreateFromBytes(ByteBuffer bytes) {
			return new IntWritable(bytes.getInt());
		}

	}
}
