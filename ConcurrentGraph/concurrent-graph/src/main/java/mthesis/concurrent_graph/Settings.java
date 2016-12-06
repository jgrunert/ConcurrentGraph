package mthesis.concurrent_graph;

import ch.qos.logback.classic.Level;

public class Settings {
	public static final boolean KEEPALIVE = true;
	public static final boolean TCP_NODELAY = true;
	public static final boolean SSL = System.getProperty("ssl") != null;
	public static final int CONNECT_TIMEOUT = 5000;
	public static final int MESSAGE_TIMEOUT = 2000;

	public static final int LOG_LEVEL_NETTY = Level.INFO_INT;
}