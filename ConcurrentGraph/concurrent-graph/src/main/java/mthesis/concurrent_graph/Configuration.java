package mthesis.concurrent_graph;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class Configuration {

	private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

	public static final String CONFIG_FILE = "configs/configuration.properties";
	public static final Properties Properties = new Properties();

	/** Maximum size of a message in bytes */
	public static final int MAX_MESSAGE_SIZE;
	/** Maximum number of messages per vertex message. Must ensure that messages not >MAX_MESSAGE_SIZE/MsgSize. */
	public static final int VERTEX_MESSAGE_BUCKET_MAX_MESSAGES; // TODO Could be not constant, depending on message content size
	/** Maximum number of vertices per vertex-move message. Must ensure that messages not >MAX_MESSAGE_SIZE/VertSize. */
	public static final int VERTEX_MOVE_BUCKET_MAX_VERTICES; // TODO Could be not constant, depending on message content size

	public static final boolean TCP_NODELAY;
	public static final int CONNECT_TIMEOUT;
	public static final int MESSAGE_TIMEOUT;


	/**
	 * Enables moving of vertices while queries are running, without barrier.
	 * TODO Broken
	 */
	public static final boolean VERTEX_LIVE_MOVE_ENABLED;

	/**
	 * When enabled, machines will discover and store mappings VertexId->Machine.
	 * This is done by sending "get-to-know-messages":
	 * A receiver of a broadcast message replies with all dstVertex IDs on its machine.
	 */
	public static final boolean VERTEX_MACHINE_DISCOVERY;
	/**
	 * If enabled, also adds discovered vertices from incoming broadcast messages.
	 * This increases learning speed but can also lead to discovered vertices but no corresponding outgoing edge.
	 */
	public static final boolean VERTEX_MACHINE_DISCOVERY_INCOMING;


	public static final int LOG_LEVEL_Main;
	//public static final int LOG_LEVEL_Main = Level.DEBUG_INT;

	/** Default size of slots for parallel queries */
	public static final int DEFAULT_QUERY_SLOTS;
	/** Maximum number parallel active of queries */
	public static final int MAX_PARALLEL_QUERIES;


	public static final boolean VERTEX_MESSAGE_POOLING;
	public static final int VERTEX_MESSAGE_POOL_SIZE;



	static {
		// Load configuration
		if (!(new File(CONFIG_FILE)).exists())
			throw new RuntimeException("Unable to start without configuration: No file found at " + CONFIG_FILE);
		try (BufferedInputStream stream = new BufferedInputStream(new FileInputStream(CONFIG_FILE))) {
			Properties.load(stream);

			// Write important values from properties file to constants to improve performance.
			// We dont want to do a map lookup on every message.
			MAX_MESSAGE_SIZE = Integer.parseInt(Properties.getProperty("MAX_MESSAGE_SIZE"));
			VERTEX_MESSAGE_BUCKET_MAX_MESSAGES = Integer.parseInt(Properties.getProperty("VERTEX_MESSAGE_BUCKET_MAX_MESSAGES"));
			VERTEX_MOVE_BUCKET_MAX_VERTICES = Integer.parseInt(Properties.getProperty("VERTEX_MOVE_BUCKET_MAX_VERTICES"));
			TCP_NODELAY = Boolean.parseBoolean(Properties.getProperty("TCP_NODELAY"));
			CONNECT_TIMEOUT = Integer.parseInt(Properties.getProperty("CONNECT_TIMEOUT"));
			MESSAGE_TIMEOUT = Integer.parseInt(Properties.getProperty("MESSAGE_TIMEOUT"));
			VERTEX_LIVE_MOVE_ENABLED = Boolean.parseBoolean(Properties.getProperty("VERTEX_LIVE_MOVE_ENABLED"));
			VERTEX_MACHINE_DISCOVERY = Boolean.parseBoolean(Properties.getProperty("VERTEX_MACHINE_DISCOVERY"));
			VERTEX_MACHINE_DISCOVERY_INCOMING = Boolean.parseBoolean(Properties.getProperty("VERTEX_MACHINE_DISCOVERY_INCOMING"));
			LOG_LEVEL_Main = Level.valueOf(Properties.getProperty("LOG_LEVEL_Main")).levelInt;
			DEFAULT_QUERY_SLOTS = Integer.parseInt(Properties.getProperty("DEFAULT_QUERY_SLOTS"));
			MAX_PARALLEL_QUERIES = Integer.parseInt(Properties.getProperty("MAX_PARALLEL_QUERIES"));
			VERTEX_MESSAGE_POOLING = Boolean.parseBoolean(Properties.getProperty("VERTEX_MESSAGE_POOLING"));
			VERTEX_MESSAGE_POOL_SIZE = Integer.parseInt(Properties.getProperty("VERTEX_MESSAGE_POOL_SIZE"));

			logger.debug("Configuration loaded from " + CONFIG_FILE);
		}
		catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException("Failure while loading configuration file " + CONFIG_FILE, e);
		}
	}


	public static boolean getPropertyBool(String propName) {
		return Boolean.parseBoolean(Properties.getProperty(propName));
	}

	public static int getPropertyInt(String propName) {
		return Integer.parseInt(Properties.getProperty(propName));
	}
}