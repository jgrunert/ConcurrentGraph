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


	public static class Factory extends BaseWritableFactory<IntWritable> {
		@Override
		public IntWritable CreateFromBytes(ByteBuffer bytes) {
			return new IntWritable(bytes.getInt());
		}

	}
}
