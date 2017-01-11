package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

/**
 * Base class for writable objects. Used for fast serialization.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class BaseWritable {

	public abstract void writeToBuffer(ByteBuffer buffer);

	public abstract int getBytesLength();

	public abstract String getString();


	public byte[] getBytes() {
		ByteBuffer buf = ByteBuffer.allocate(getBytesLength());
		writeToBuffer(buf);
		return buf.array();
	}



	public static abstract class BaseWritableFactory<T extends BaseWritable> {

		public abstract T createFromString(String str);

		public abstract T createFromBytes(ByteBuffer bytes);

		public T createClone(T toClone) {
			return createFromBytes(ByteBuffer.wrap(toClone.getBytes()));
		}
	}

	@Override
	public String toString() {
		return getString();
	}
}
