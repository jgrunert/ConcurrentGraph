package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

public class NullWritable extends BaseWritable {

	public NullWritable() {
		super();
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
	}

	@Override
	public String getString() {
		return "";
	}


	public static class Factory extends BaseWritableFactory<NullWritable> {
		@Override
		public NullWritable createFromString(String str) {
			return new NullWritable();
		}

		@Override
		public NullWritable createFromBytes(ByteBuffer bytes) {
			return new NullWritable();
		}
	}
}
