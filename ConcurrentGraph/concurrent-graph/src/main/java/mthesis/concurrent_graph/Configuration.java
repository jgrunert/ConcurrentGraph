package mthesis.concurrent_graph;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class Configuration {

	private static Logger logger = LoggerFactory.getLogger(Configuration.class);
	public static String ConfigFile;
	public static Properties Properties = new Properties();

	public static final String VERSION = "0.0.12";

	/** Maximum size of a message in bytes */
	public static int MAX_MESSAGE_SIZE;
	/** Maximum number of messages per vertex message. Must ensure that messages not >MAX_MESSAGE_SIZE/MsgSize. */
	public static int VERTEX_MESSAGE_BUCKET_MAX_MESSAGES; // TODO Could be not constant, depending on message content size
	/** Maximum number of vertices per vertex-move message. Must ensure that messages not >MAX_MESSAGE_SIZE/VertSize. */
	public static int VERTEX_MOVE_BUCKET_MAX_VERTICES; // TODO Could be not constant, depending on message content size

	public static boolean TCP_NODELAY;
	public static int CONNECT_TIMEOUT;
	public static int MESSAGE_TIMEOUT;


	/**
	 * Enables moving of vertices while queries are running, without barrier.
	 * TODO Broken/NotImplemented
	 */
	public static boolean VERTEX_LIVE_MOVE_ENABLED;

	public static boolean VERTEX_BARRIER_MOVE_ENABLED;
	public static long VERTEX_BARRIER_MOVE_INTERVAL;

	/**
	 * When enabled, machines will discover and store mappings VertexId->Machine.
	 * This is done by sending "get-to-know-messages":
	 * A receiver of a broadcast message replies with all dstVertex IDs on its machine.
	 */
	public static boolean VERTEX_MACHINE_DISCOVERY;
	/**
	 * If enabled, also adds discovered vertices from incoming broadcast messages.
	 * This increases learning speed but can also lead to discovered vertices but no corresponding outgoing edge.
	 */
	public static boolean VERTEX_MACHINE_DISCOVERY_INCOMING;

	public static int LOG_LEVEL_MAIN;
	//public static int LOG_LEVEL_Main = Level.DEBUG_INT;

	/** Default size of slots for parallel queries */
	public static int DEFAULT_QUERY_SLOTS;
	/** Maximum number parallel active of queries */
	public static int MAX_PARALLEL_QUERIES;

	public static boolean VERTEX_MESSAGE_POOLING;
	public static int VERTEX_MESSAGE_POOL_SIZE;

	public static int WORKER_WATCHDOG_TIME;

	public static int WORKER_STATS_SAMPLING_INTERVAL;
	// Enables recording of some more expensive stats
	public static boolean DETAILED_STATS;



	public static void loadConfig(String configFile) {
		// Load configuration
		if (!(new File(configFile)).exists())
			throw new RuntimeException("Unable to start without configuration: No file found at " + configFile);
		try (BufferedInputStream stream = new BufferedInputStream(new FileInputStream(configFile))) {
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
			VERTEX_BARRIER_MOVE_ENABLED = Boolean.parseBoolean(Properties.getProperty("VERTEX_BARRIER_MOVE_ENABLED"));
			VERTEX_BARRIER_MOVE_INTERVAL = Long.parseLong(Properties.getProperty("VERTEX_BARRIER_MOVE_INTERVAL"));
			VERTEX_MACHINE_DISCOVERY = Boolean.parseBoolean(Properties.getProperty("VERTEX_MACHINE_DISCOVERY"));
			VERTEX_MACHINE_DISCOVERY_INCOMING = Boolean.parseBoolean(Properties.getProperty("VERTEX_MACHINE_DISCOVERY_INCOMING"));
			LOG_LEVEL_MAIN = Level.valueOf(Properties.getProperty("LOG_LEVEL_MAIN")).levelInt;
			DEFAULT_QUERY_SLOTS = Integer.parseInt(Properties.getProperty("DEFAULT_QUERY_SLOTS"));
			MAX_PARALLEL_QUERIES = Integer.parseInt(Properties.getProperty("MAX_PARALLEL_QUERIES"));
			VERTEX_MESSAGE_POOLING = Boolean.parseBoolean(Properties.getProperty("VERTEX_MESSAGE_POOLING"));
			VERTEX_MESSAGE_POOL_SIZE = Integer.parseInt(Properties.getProperty("VERTEX_MESSAGE_POOL_SIZE"));
			WORKER_WATCHDOG_TIME = Integer.parseInt(Properties.getProperty("WORKER_WATCHDOG_TIME"));
			DETAILED_STATS = Boolean.parseBoolean(Properties.getProperty("DETAILED_STATS"));
			WORKER_STATS_SAMPLING_INTERVAL = Integer.parseInt(Properties.getProperty("WORKER_STATS_SAMPLING_INTERVAL"));

			ConfigFile = configFile;
			logger.info("Configuration loaded from " + configFile);
			logger.info("Log level " + Level.toLevel(LOG_LEVEL_MAIN));
			System.out.println("Log level " + Level.toLevel(LOG_LEVEL_MAIN));
		}
		catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException("Failure while loading configuration file " + configFile, e);
		}
	}


	public static boolean getPropertyBool(String propName) {
		return Boolean.parseBoolean(Properties.getProperty(propName));
	}

	public static int getPropertyInt(String propName) {
		return Integer.parseInt(Properties.getProperty(propName));
	}

	public static long getPropertyLong(String propName) {
		return Long.parseLong(Properties.getProperty(propName));
	}

	public static long getPropertyLongDefault(String propName, long defaultValue) {
		if (!Properties.containsKey(propName)) return defaultValue;
		return Long.parseLong(Properties.getProperty(propName));
	}
}