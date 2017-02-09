package mthesis.concurrent_graph.communication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Sends asynchronous messages on a channel to another machine. Runs a sender thread.
 *
 * @author Jonas Grunert
 *
 */
public class ChannelAsyncMessageSender<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	private final Logger logger;
	private final Socket socket;
	private final OutputStream writer;
	private final byte[] outBytes = new byte[Settings.MAX_MESSAGE_SIZE];
	private final ByteBuffer outBuffer = ByteBuffer.wrap(outBytes);
	private final BlockingQueue<ChannelMessage> outMessages = new LinkedBlockingQueue<>();
	private Thread senderThread;


	public ChannelAsyncMessageSender(Socket socket, OutputStream writer, int ownId) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.writer = writer;
	}

	public void startSender(int ownId, int otherId) {
		senderThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					while (!Thread.interrupted() && !socket.isClosed()) {
						final ChannelMessage message = outMessages.take();
						sendMessageViaStream(message);
					}
				}
				catch (final InterruptedException e2) {
					return;
				}
				catch (final Exception e) {
					if (!socket.isClosed())
						logger.error("sending failed", e);
				}
				finally {
					logger.debug("sender finished");
				}
			}
		});
		senderThread.setName("ChannelAsyncSenderThread_" + ownId + "_" + otherId);
		senderThread.setDaemon(true);
		senderThread.start();
	}

	// Sends message via stream
	// THREADING NOTE: Not threadsafe
	// Format: short MsgLength, byte MsgType, byte[] MsgContent
	private void sendMessageViaStream(final ChannelMessage message) throws IOException {
		if (message.hasContent()) {
			outBuffer.position(2); // Leave 2 bytes for content length
			outBuffer.put(message.getTypeCode());
			message.writeMessageToBuffer(outBuffer);
			// Write position
			final int msgLength = outBuffer.position();

			outBuffer.position(0);
			outBuffer.putShort((short) (msgLength - 2));
			// Send message
			writer.write(outBytes, 0, msgLength);
			outBuffer.clear();
		}
		if (message.flushAfter()) {
			writer.flush();
		}
	}

	public void close() {
		try {
			if (!socket.isClosed())
				socket.close();
		}
		catch (final IOException e) {
			logger.error("close socket failed", e);
		}
		flush();
		senderThread.interrupt();
	}


	/**
	 * Sends a message through the channel asynchronously, without acknowledgement
	 */
	public void sendMessageAsync(ChannelMessage message) {
		outMessages.add(message);
	}

	public void flush() {
		outMessages.add(new FlushDummyMessage());
	}



	private class FlushDummyMessage implements ChannelMessage {

		@Override
		public boolean hasContent() {
			return false;
		}

		@Override
		public boolean flushAfter() {
			return true;
		}

		@Override
		public byte getTypeCode() {
			throw new RuntimeException("Not supported for FlushDummyMessage");
		}

		@Override
		public void writeMessageToBuffer(ByteBuffer buffer) {
			throw new RuntimeException("Not supported for FlushDummyMessage");
		}
	}
}
