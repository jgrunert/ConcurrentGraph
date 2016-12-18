package mthesis.concurrent_graph.communication;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Sends messages on a channel to another machine. Runs a sender thread.
 * 
 * @author Jonas Grunert
 *
 */
public class ChannelMessageSender<M extends BaseWritable> {
	private final Logger logger;
	private final Socket socket;
	private final DataOutputStream writer;
	private final byte[] outBytes = new byte[Settings.MAX_MESSAGE_SIZE];
	private final ByteBuffer outBuffer = ByteBuffer.wrap(outBytes);
	private final BlockingQueue<MessageToSend> outMessages = new LinkedBlockingQueue<>();
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

						// Format: short MsgLength, byte MsgType, byte[] MsgContent
						if(message.hasContent()){
							outBuffer.put(message.getTypeCode());
							message.writeMessageToBuffer(outBuffer);
							final int msgLength = outBuffer.position();
							writer.writeShort((short)msgLength);
							writer.write(outBytes, 0, msgLength);
							outBuffer.clear();
						}
						if(message.flushAfter()) {
							writer.flush();
						}
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
		flush();
		senderThread.interrupt();
		// TODO Flush messages? join?
	}


	public void sendMessageEnvelope(MessageEnvelope message, boolean flush) {
		outMessages.add(new MessageEnvelopeToSend(message, flush));  // TODO Object pooling?
	}

	public void sendVertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag, List<Pair<Integer, M>> vertexMessages) {
		outMessages.add(new VertexMessageToSend(superstepNo, srcMachine, broadcastFlag, vertexMessages));  // TODO Object pooling?
	}

	public void flush() {
		outMessages.add(new FlushDummyMessage());  // TODO Object pooling?
	}


	private interface MessageToSend {
		boolean hasContent();
		boolean flushAfter();
		byte getTypeCode();
		void writeMessageToBuffer(ByteBuffer buffer);
	}

	private class VertexMessageToSend implements MessageToSend {
		private final int superstepNo;
		private final int srcMachine;
		private final boolean broadcastFlag;
		private final List<Pair<Integer, M>> vertexMessages;

		public VertexMessageToSend(int superstepNo, int srcMachine, boolean broadcastFlag, List<Pair<Integer, M>> vertexMessages) {
			this.srcMachine = srcMachine;
			this.superstepNo = superstepNo;
			this.broadcastFlag = broadcastFlag;
			this.vertexMessages = vertexMessages;
		}

		@Override
		public byte getTypeCode() {
			return 0;
		}

		@Override
		public void writeMessageToBuffer(ByteBuffer buffer) {
			buffer.putInt(superstepNo);
			buffer.putInt(srcMachine);
			buffer.put(broadcastFlag ? (byte)0 : (byte)1);
			buffer.putInt(vertexMessages.size());
			for(final Pair<Integer, M> msg : vertexMessages) {
				buffer.putInt(msg.first);
				msg.second.writeToBuffer(buffer);
			}
		}

		@Override
		public boolean hasContent() {
			return true;
		}

		@Override
		public boolean flushAfter() {
			return false;
		}
	}

	private class MessageEnvelopeToSend implements MessageToSend {
		private final MessageEnvelope message;
		private final boolean flushAfter;

		public MessageEnvelopeToSend(MessageEnvelope message, boolean flushAfter) {
			this.message = message;
			this.flushAfter = flushAfter;
		}

		@Override
		public byte getTypeCode() {
			return 1;
		}

		@Override
		public void writeMessageToBuffer(ByteBuffer buffer) {
			buffer.put(message.toByteArray());
		}


		@Override
		public boolean hasContent() {
			return true;
		}

		@Override
		public boolean flushAfter() {
			return flushAfter;
		}
	}

	private class FlushDummyMessage implements MessageToSend {
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
