package mthesis.concurrent_graph.communication;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.writable.BaseWritable;


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
	private final byte[] outBytes = new byte[Settings.MAX_MESSAGE_SIZE];
	private final ByteBuffer outBuffer = ByteBuffer.wrap(outBytes);
	private final BlockingQueue<MessageToSend> outMessages = new LinkedBlockingQueue<>();  // TODO Pool MessageToSend objects?
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
						final MessageToSend message = outMessages.take();
						outBuffer.clear();

						// Format: short MsgLength, byte MsgType, byte[] MsgContent
						outBuffer.put(message.getTypeCode());
						message.writeMessageToBuffer(outBuffer);
						final int msgLength = outBuffer.position();
						writer.writeShort((short)msgLength);
						writer.write(outBytes, 0, msgLength);
					}
				}
				catch(final InterruptedException e2) {
					return;
				}
				catch(final Exception e) {
					if(!socket.isClosed())
						logger.error("sending failed", e);
				}
				finally {
					logger.debug("sender finished");
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


	// TODO Handle flush etc
	public void sendMessageEnvelope(MessageEnvelope message, boolean flush) {
		outMessages.add(new MessageEnvelopeToSend(message));
	}

	// TODO Handle flush etc
	// TODO Remove srcVertex? Use it for discovery?
	public void sendVertexMessage(BaseWritable message, int superstepNo, int srcMachine, int srcVertex, int dstVertex,  boolean flush) {
		// TODO Buckets etc
		outMessages.add(new VertexMessageToSend(message, superstepNo, srcMachine, srcVertex, dstVertex));
	}


	private interface MessageToSend {
		byte getTypeCode();
		void writeMessageToBuffer(ByteBuffer buffer);
	}

	private class VertexMessageToSend implements MessageToSend {
		private final BaseWritable message;
		private final int superstepNo;
		private final int srcMachine;
		private final int srcVertex;
		private final int dstVertex;

		public VertexMessageToSend(BaseWritable message, int superstepNo, int srcMachine, int srcVertex, int dstVertex) {
			this.message = message;
			this.srcMachine = srcMachine;
			this.superstepNo = superstepNo;
			this.srcVertex = srcVertex;
			this.dstVertex = dstVertex;
		}

		@Override
		public byte getTypeCode() {
			return 0;
		}

		@Override
		public void writeMessageToBuffer(ByteBuffer buffer) {
			buffer.putInt(superstepNo);
			buffer.putInt(srcMachine);
			buffer.putInt(srcVertex);
			buffer.putInt(dstVertex);
			if(message != null) {
				buffer.put((byte)0); // TODO Remove this option for null?
				message.writeToBuffer(buffer);
			}
			else {
				buffer.put((byte)1);
			}
		}
	}

	private class MessageEnvelopeToSend implements MessageToSend {
		private final MessageEnvelope message;

		public MessageEnvelopeToSend(MessageEnvelope message) {
			this.message = message;
		}

		@Override
		public byte getTypeCode() {
			return 1;
		}

		@Override
		public void writeMessageToBuffer(ByteBuffer buffer) {
			buffer.put(message.toByteArray());
		}
	}
}
