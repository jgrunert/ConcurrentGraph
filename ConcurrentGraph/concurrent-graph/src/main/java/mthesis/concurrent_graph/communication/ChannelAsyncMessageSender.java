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
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Sends asynchronous messages on a channel to another machine. Runs a sender thread.
 *
 * @author Jonas Grunert
 *
 */
public class ChannelAsyncMessageSender<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public static final int ChannelCloseSignal = -2;

	private final Logger logger;
	private final Socket socket;
	private final OutputStream writer;
	private final byte[] outBytes = new byte[Configuration.MAX_MESSAGE_SIZE];
	private final ByteBuffer outBuffer = ByteBuffer.wrap(outBytes);
	private final BlockingQueue<ChannelMessage> outMessages = new LinkedBlockingQueue<>();
	private Thread senderThread;
	private boolean isClosing = false;


	public ChannelAsyncMessageSender(Socket socket, OutputStream writer, int ownId) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.writer = writer;
	}

	// TODO Testcode
	//	ChannelMessage m0 = null;
	//	ChannelMessage m1 = null;

	public void startSender(int ownId, int otherId) {

		senderThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					while (!Thread.interrupted() && !socket.isClosed()) {
						if (isClosing && outMessages.isEmpty())
							break;
						final ChannelMessage message = outMessages.take();
						//						m1 = m0;
						//						m0 = message;
						sendMessageViaStream(message);
						message.free(true);
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
					//					logger.debug("sender finished + " + m0 + " " + m1);
					logger.debug("sender finished");
				}
			}
		});
		senderThread.setName("ChannelAsyncSenderThread_" + ownId + "_" + otherId);
		senderThread.setDaemon(true);
		senderThread.start();
	}

	// Sends message via stream.
	// THREADING NOTE: Not threadsafe
	// Format: short MsgLength, byte MsgType, byte[] MsgContent
	private void sendMessageViaStream(final ChannelMessage message) throws IOException {
		if (message.hasContent()) {
			outBuffer.clear();
			outBuffer.position(4); // Leave 2 bytes for content length
			outBuffer.put(message.getTypeCode());
			message.writeMessageToBuffer(outBuffer);

			final int msgLength = outBuffer.position();
			if (msgLength <= 0) {
				logger.error("Unable to send message with non positive length " + msgLength);
				return;
			}
			if ((msgLength + 4) > Configuration.MAX_MESSAGE_SIZE) {
				logger.error("Unable to send message with too long length " + msgLength);
				return;
			}

			outBuffer.position(0);
			outBuffer.putInt((msgLength - 4));
			//			synchronized (writer)
			{
				// Send message
				writer.write(outBytes, 0, msgLength);
			}

			// TODO Temporary check to find messaging issues
			outBuffer.position(0);
			int testLen = outBuffer.getInt();
			if (testLen != msgLength - 4) {
				logger.warn("Wrong overwritten length, " + testLen + " instead of " + (msgLength - 4));
			}
		}
		if (message.flushAfter()) {
			//			synchronized (writer)
			{
				writer.flush();
			}
		}
	}

	public void close() {
		try {
			if (!socket.isClosed()) {
				// Stop sending
				isClosing = true;
				flush();
				while (!outMessages.isEmpty())
					Thread.sleep(1);
				senderThread.interrupt();
				senderThread.join(100);

				// Send close signal if not closed now
				if (!socket.isClosed()) {
					outBuffer.clear();
					outBuffer.position(0);
					outBuffer.putInt(ChannelCloseSignal);
					//					synchronized (writer)
					{
						writer.write(outBytes, 0, 4);
						writer.flush();
					}

					socket.close();
				}
			}
		}
		catch (final Exception e) {
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

		@Override
		public void free(boolean freeMembers) {
		}
	}
}
