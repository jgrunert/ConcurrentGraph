package mthesis.concurrent_graph.communication;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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
	private final Socket socket;
	private final DataOutputStream writer;
	private final BlockingQueue<MessageEnvelope> outMessages = new LinkedBlockingQueue<>();

	private Thread senderThread;


	public ChannelMessageSender(Socket socket, DataOutputStream writer, int ownId) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.writer = writer;
	}

	public void startSender(int ownId, int otherId) {
		senderThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try{
					while(!Thread.interrupted() && !socket.isClosed()) {
						final MessageEnvelope message = outMessages.take();
						final byte[] msgBytes = message.toByteArray();
						writer.writeShort((short)msgBytes.length);
						writer.write(msgBytes, 0, msgBytes.length);
					}
				}
				catch(final Exception e) {
					logger.error("sending failed", e);
				}
			}
		});
		senderThread.setName("SenderThread_" + ownId + "_" + otherId);
		senderThread.setDaemon(true);
		senderThread.start();
	}

	public void close() {
		try {
			if(!socket.isClosed())
				socket.close();
		}
		catch (final IOException e) {
			logger.error("close socket failed", e);
		}
		senderThread.interrupt();
		// TODO Flush messages? join?
	}

	public void sendMessage(MessageEnvelope message, boolean flush) {  // TODO Not synchronized, use buckets, async etc.
		// TODO Buckets etc
		// TODO Handle flush or remove it
		outMessages.add(message);
	}
}
