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

	@Override
	public int getBytesLength() {
		return 4;
	}


	public static class Factory extends BaseWritableFactory<IntWritable> {

		@Override
		public IntWritable createDefault() {
			return new IntWritable(0);
		}

		@Override
		public IntWritable createFromString(String str) {
			return new IntWritable(Integer.parseInt(str));
		}

		@Override
		public IntWritable createFromBytes(ByteBuffer bytes) {
			return new IntWritable(bytes.getInt());
		}

		@Override
		public IntWritable createClone(IntWritable toClone) {
			return new IntWritable(toClone.Value);
		}
	}
}
