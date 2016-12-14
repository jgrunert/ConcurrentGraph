package mthesis.concurrent_graph.writable;

import java.nio.ByteBuffer;

/**
 * Base class for writable objects. Used for fast serialization.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class BaseWritable {
	public abstract ByteBuffer GetBytes();
	public abstract String GetString();


	public static abstract class BaseWritableFactory<T>{
		public abstract T CreateFromString(String str);
		public abstract T CreateFromBytes(ByteBuffer bytes);
	}
}
