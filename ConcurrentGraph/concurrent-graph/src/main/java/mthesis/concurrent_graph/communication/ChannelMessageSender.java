package mthesis.concurrent_graph.communication;

import java.io.DataOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
	private final BlockingQueue<MessageEnvelope> outMessages = new LinkedBlockingQueue<>();


	public ChannelMessageSender(DataOutputStream writer, int ownId) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.writer = writer;
	}

	public synchronized void sendMessage(MessageEnvelope message, boolean flush) {  // TODO Not synchronized, use buckets, async etc.
		// TODO Buckets etc
		outMessages.add(message);

		final byte[] msgBytes = message.toByteArray();
		try {
			writer.writeShort((short)msgBytes.length);
			writer.write(msgBytes, 0, msgBytes.length);
			if(flush)
				writer.flush();
		}
		catch (final Exception e) {
			logger.error("send failed", e);
		}
	}
}
