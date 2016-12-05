package mthesis.concurrent_graph;

public class Settings {
	public static final boolean KEEPALIVE = true;
	public static final boolean TCP_NODELAY = true;
	public static final boolean SSL = System.getProperty("ssl") != null;	
	public static final int CONNECT_TIMEOUT = 2000;
}