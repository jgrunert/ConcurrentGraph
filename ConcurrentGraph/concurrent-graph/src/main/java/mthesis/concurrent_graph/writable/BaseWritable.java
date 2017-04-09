package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for writable objects. Used for fast serialization.
 *
 * @author Jonas Grunert
 *
 */
public abstract class BaseWritable {

	protected static final Logger logger = LoggerFactory.getLogger(BaseWritable.class);

	public abstract void readFromBuffer(ByteBuffer buffer);

	public abstract void writeToBuffer(ByteBuffer buffer);

	public abstract int getBytesLength();

	public abstract String getString();


	public byte[] getBytes() {
		ByteBuffer buf = ByteBuffer.allocate(getBytesLength());
		writeToBuffer(buf);
		return buf.array();
	}



	public static abstract class BaseWritableFactory<T extends BaseWritable> {

		public abstract T createDefault();

		public abstract T createFromString(String str);

		public T createFromBytes(ByteBuffer bytes) {
			T created = createDefault();
			created.readFromBuffer(bytes);
			return created;
		}

		public T createClone(T toClone) {
			return createFromBytes(ByteBuffer.wrap(toClone.getBytes()));
		}
	}

	@Override
	public String toString() {
		return getString();
	}
}
