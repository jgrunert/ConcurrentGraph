package mthesis.concurrent_graph;

import ch.qos.logback.classic.Level;

public class Settings {
	public static final boolean KEEPALIVE = true;
	public static final boolean TCP_NODELAY = true;
	public static final boolean SSL = System.getProperty("ssl") != null;
	public static final int CONNECT_TIMEOUT = 10000;
	public static final int MESSAGE_TIMEOUT = 6000;

	public static final int LOG_LEVEL_Main = Level.DEBUG_INT;
	public static final int LOG_LEVEL_NETTY = Level.INFO_INT;
}