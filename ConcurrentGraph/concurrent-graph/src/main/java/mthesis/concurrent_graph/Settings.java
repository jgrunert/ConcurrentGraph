package mthesis.concurrent_graph;

import ch.qos.logback.classic.Level;

public class Settings {

	public static final int MAX_MESSAGE_SIZE = 32768;

	/** Maximum number of messages per vertex message. Must ensure that messages not >MAX_MESSAGE_SIZE. */
	public static final int VERTEX_MESSAGE_BUCKET_MAX_MESSAGES = 512; // TODO Could be not constant, depending on message content size

	//	public static final boolean KEEPALIVE = true;
	public static final boolean TCP_NODELAY = true;
	//	public static final boolean SSL = System.getProperty("ssl") != null;
	public static final int CONNECT_TIMEOUT = 10000;
	public static final int MESSAGE_TIMEOUT = 6000;

	/**
	 * When enabled, machines will discover and store mappings VertexId->Machine.
	 * This is done by sending "get-to-know-messages":
	 * A receiver of a broadcast message replies with all dstVertex IDs on its machine.
	 */
	public static final boolean VERTEX_MACHINE_DISCOVERY = true;
	/**
	 * If enabled, also adds discovered vertices from incoming broadcast messages.
	 * This increases learning speed but can also lead to discovered vertices but no corresponding outgoing edge.
	 */
	public static final boolean VERTEX_MACHINE_DISCOVERY_INCOMING = false;

	public static final int LOG_LEVEL_Main = Level.DEBUG_INT;
}