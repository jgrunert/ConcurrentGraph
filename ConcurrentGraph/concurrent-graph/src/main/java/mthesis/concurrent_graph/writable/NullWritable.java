package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

public class NullWritable extends BaseWritable {

	public NullWritable() {
		super();
	}


	@Override
	public ByteBuffer GetBytes() {
		return ByteBuffer.allocate(0);
	}

	@Override
	public String GetString() {
		return "";
	}


	public static class Factory extends BaseWritableFactory<NullWritable> {
		@Override
		public NullWritable CreateFromString(String str) {
			return new NullWritable();
		}

		@Override
		public NullWritable CreateFromBytes(ByteBuffer bytes) {
			return new NullWritable();
		}
	}
}
