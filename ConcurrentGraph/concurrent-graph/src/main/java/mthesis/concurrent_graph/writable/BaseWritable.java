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
	public abstract String getString();


	public static abstract class BaseWritableFactory<T>{
		public abstract T createFromString(String str);
		public abstract T createFromBytes(ByteBuffer bytes);
	}
}
