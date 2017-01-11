package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

public class DoubleWritable extends BaseWritable {

	public double Value;

	public DoubleWritable(double value) {
		super();
		Value = value;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putDouble(Value);
	}

	@Override
	public String getString() {
		return Double.toString(Value);
	}

	@Override
	public int getBytesLength() {
		return 8;
	}


	public static class Factory extends BaseWritableFactory<DoubleWritable> {

		@Override
		public DoubleWritable createFromString(String str) {
			return new DoubleWritable(Double.parseDouble(str));
		}

		@Override
		public DoubleWritable createFromBytes(ByteBuffer bytes) {
			return new DoubleWritable(bytes.getDouble());
		}
	}
}
