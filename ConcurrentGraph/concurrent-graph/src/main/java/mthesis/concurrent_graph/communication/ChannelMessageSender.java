package mthesis.concurrent_graph.communication;

import java.io.DataOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;


/**
 * Sends messages on a channel to another machine. Runs a receive thread.
 * 
 * @author Jonas Gruenrt
 *
 */
public class ChannelMessageSender {
	private final Logger logger;
	private final DataOutputStream writer;


	public ChannelMessageSender(DataOutputStream writer, int ownId) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.writer = writer;
	}

	public void sendMessage(MessageEnvelope message, boolean flush) {
		// TODO Buckets etc
		final byte[] msgBytes = message.toByteArray();
		if(msgBytes.length > 1000)
			System.err.println(msgBytes.length);
		try {
			writer.writeInt(msgBytes.length);
			writer.write(msgBytes);
			if(flush)
				writer.flush();
		}
		catch (final Exception e) {
			logger.error("send failed", e);
		}
	}
}
