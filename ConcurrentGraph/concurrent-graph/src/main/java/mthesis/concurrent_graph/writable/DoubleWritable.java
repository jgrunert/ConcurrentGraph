package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

public class DoubleWritable extends BaseWritable {

	public double Value;

	public DoubleWritable(double value) {
		super();
		Value = value;
	}


	@Override
	public ByteBuffer GetBytes() {
		final ByteBuffer bytes = ByteBuffer.allocate(8);
		bytes.putDouble(Value);
		return bytes;
	}

	@Override
	public String GetString() {
		return Double.toString(Value);
	}


	public static class Factory extends BaseWritableFactory<DoubleWritable> {
		@Override
		public DoubleWritable CreateFromString(String str) {
			return new DoubleWritable(Double.parseDouble(str));
		}

		@Override
		public DoubleWritable CreateFromBytes(ByteBuffer bytes) {
			return new DoubleWritable(bytes.getDouble());
		}

	}
}
