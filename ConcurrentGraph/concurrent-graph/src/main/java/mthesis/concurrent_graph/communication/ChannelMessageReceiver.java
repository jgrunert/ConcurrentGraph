package mthesis.concurrent_graph.communication;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Receives messages on a channel from another machine. Runs a receive thread.
 * 
 * @author Jonas Grunert
 *
 */
public class ChannelMessageReceiver<M extends BaseWritable> {

	private final Logger logger;
	private final int ownId;
	private final Socket socket;
	private final InputStream reader;
	private final byte[] inBytes = new byte[Settings.MAX_MESSAGE_SIZE];
	private final ByteBuffer inBuffer = ByteBuffer.wrap(inBytes);
	private final MessageSenderAndReceiver<M> inMsgHandler;
	private final BaseWritable.BaseWritableFactory<M> vertexMessageFactory;
	private Thread thread;
	private volatile boolean readyForClose;

	public ChannelMessageReceiver(Socket socket, InputStream reader, int ownId,
			MessageSenderAndReceiver<M> inMsgHandler, BaseWritable.BaseWritableFactory<M> vertexMessageFactory) {
		this.ownId = ownId;
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.reader = reader;
		this.inMsgHandler = inMsgHandler;
		this.vertexMessageFactory = vertexMessageFactory;
	}

	public void startReceiver(int connectedMachineId) {
		readyForClose = false;
		thread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					int msgContentLength;
					int readIndex;
					while (!Thread.interrupted() && !socket.isClosed()) {
						reader.read(inBytes, 0, 2);
						msgContentLength = inBuffer.getShort();

						inBuffer.clear();
						readIndex = 0;
						while (readIndex < msgContentLength) {
							readIndex += reader.read(inBytes, readIndex, msgContentLength - readIndex);
							if (readIndex == -1) {
								logger.debug("Reader returned -1, exiting reader");
								return;
							}
						}

						final byte msgType = inBuffer.get();
						switch (msgType) {
							case 0:
								onIncomingVertexMessage();
								break;
							case 1:
								onIncomingControlMessage(1, msgContentLength - 1);
								break;
							case 2:
								onIncomingGetToKnowMessage();
								break;

							default:
								logger.warn("Unknown incoming message id: " + msgType);
								break;
						}
						inBuffer.clear();
					}
				}
				catch (final Exception e) {
					if (!readyForClose) {
						if (socket.isClosed()) logger.debug("Socket closed");
						else logger.error("receive error", e);
					}
				}
				finally {
					try {
						if (!socket.isClosed()) socket.close();
					}
					catch (final IOException e) {
						logger.error("close socket failed", e);
					}
					logger.debug("ChannelMessageReceiver closed");
				}
			}
		});
		thread.setDaemon(true);
		thread.setName("ChannelReceiver_" + ownId + "_" + connectedMachineId);
		thread.start();
	}

	private void onIncomingVertexMessage() {
		final int msgSuperstepNo = inBuffer.getInt();
		final int srcMachine = inBuffer.getInt();
		final boolean broadcastFlag = inBuffer.get() == 0;
		final int vertexMessagesCount = inBuffer.getInt();
		final List<Pair<Integer, M>> vertexMessages = new ArrayList<>(vertexMessagesCount); // TODO
																							// Pool
																							// instances?
		for (int i = 0; i < vertexMessagesCount; i++) {
			vertexMessages.add(new Pair<Integer, M>(inBuffer.getInt(), vertexMessageFactory.createFromBytes(inBuffer)));
		}
		inMsgHandler.onIncomingVertexMessage(msgSuperstepNo, srcMachine, broadcastFlag, vertexMessages);
	}

	private void onIncomingControlMessage(int offset, int size) throws InvalidProtocolBufferException {
		final MessageEnvelope messageEnv = MessageEnvelope.parseFrom(ByteString.copyFrom(inBytes, offset, size));
		if (messageEnv.hasControlMessage()) inMsgHandler.onIncomingControlMessage(messageEnv.getControlMessage());
	}

	private void onIncomingGetToKnowMessage() {
		final int srcMachine = inBuffer.getInt();
		final int vertCount = inBuffer.getInt();
		final List<Integer> srcVertices = new ArrayList<>(vertCount);
		for (int i = 0; i < vertCount; i++) {
			srcVertices.add(inBuffer.getInt());
		}
		inMsgHandler.onIncomingGetToKnowMessage(srcMachine, srcVertices);
	}

	public void getReadyForClose() {
		readyForClose = true;
	}

	public void close() {
		readyForClose = true;
		try {
			if (!socket.isClosed()) socket.close();
		}
		catch (final IOException e) {
			logger.error("close socket failed", e);
		}
		thread.interrupt();
	}
}
