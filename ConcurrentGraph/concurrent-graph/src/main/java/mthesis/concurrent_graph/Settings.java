package mthesis.concurrent_graph;

import ch.qos.logback.classic.Level;

public class Settings {
	public static final boolean KEEPALIVE = true;
	public static final boolean TCP_NODELAY = false;
	public static final boolean SSL = System.getProperty("ssl") != null;
	public static final int CONNECT_TIMEOUT = 10000;
	public static final int MESSAGE_TIMEOUT = 60000;

	/**
	 * When vertex discovery is enabled, machines will store mappings for VertexId->Machine.
	 * If ACTIVE_VERTEX_DISCOVERY not enabled, only passive discovery is active.
	 * Passive discovery will register mappings for received vertex messages.
	 */
	public static final boolean VERTEX_DISCOVERY = true;
	/**
	 * If VERTEX_DISCOVERY && ACTIVE_VERTEX_DISCOVERY active discovery is enabled.
	 * Active discovery will send a "get-to-know message" back to newly discovered mappings.
	 */
	public static final boolean ACTIVE_VERTEX_DISCOVERY = true;

	public static final int LOG_LEVEL_Main = Level.DEBUG_INT;
	public static final int LOG_LEVEL_NETTY = Level.WARN_INT;
}