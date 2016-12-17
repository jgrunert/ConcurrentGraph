package mthesis.concurrent_graph.communication;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Receives messages on a channel from another machine. Runs a receive thread.
 * 
 * @author Jonas Gruenrt
 *
 */
public class ChannelMessageReceiver<M extends BaseWritable> {
	private final Logger logger;
	private final int ownId;
	private final Socket socket;
	private final DataInputStream reader;
	private final byte[] inBytes = new byte[Settings.MAX_MESSAGE_SIZE];
	private final ByteBuffer inBuffer = ByteBuffer.wrap(inBytes);
	private final MessageSenderAndReceiver<M> inMsgHandler;
	private final BaseWritable.BaseWritableFactory<M> vertexMessageFactory;
	private Thread thread;

	public ChannelMessageReceiver(Socket socket, DataInputStream reader, int ownId,
			MessageSenderAndReceiver<M> inMsgHandler, BaseWritable.BaseWritableFactory<M> vertexMessageFactory) {
		this.ownId = ownId;
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.reader = reader;
		this.inMsgHandler = inMsgHandler;
		this.vertexMessageFactory = vertexMessageFactory;
	}

	int correctRecM = 0;
	int correctRecBts = 0;
	public void startReceiver(int connectedMachineId) {
		thread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					int msgLength, readIndex;
					while(!Thread.interrupted() && !socket.isClosed()) {
						msgLength = reader.readShort();

						readIndex = 0;
						while(readIndex < msgLength) {
							readIndex += reader.read(inBytes, readIndex, msgLength - readIndex);
						}

						inBuffer.clear();
						final byte msgType = inBuffer.get();
						switch (msgType) {
							case 0:
								onIncomingVertexMessage();
								break;
							case 1:
								onIncomingControlMessage(1, msgLength - 1);
								break;

							default:
								break;
						}

						correctRecM++;
						correctRecBts += msgLength;
					}
				}
				catch(final EOFException e2) {
					// TODO Check if shutdown
				}
				catch (final Exception e) {
					if(!Thread.interrupted() && !socket.isClosed())
						logger.error("receive error", e);
				} finally{
					try {
						if(!socket.isClosed())
							socket.close();
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
		final int srcVertex = inBuffer.getInt();
		final int dstVertex = inBuffer.getInt();
		final boolean isNotNull = inBuffer.get() == 0;
		if(isNotNull) {
			final M messageContent = vertexMessageFactory.createFromBytes(inBuffer);
			inMsgHandler.onIncomingVertexMessage(msgSuperstepNo, srcMachine, srcVertex, dstVertex, messageContent);
		}
		else {
			inMsgHandler.onIncomingVertexMessage(msgSuperstepNo, srcMachine, srcVertex, dstVertex, null);
		}
	}

	private void onIncomingControlMessage(int offset, int size) throws InvalidProtocolBufferException {
		final MessageEnvelope messageEnv = MessageEnvelope.parseFrom(ByteString.copyFrom(inBytes, offset, size));
		if(messageEnv.hasControlMessage())
			inMsgHandler.onIncomingControlMessage(messageEnv.getControlMessage());
	}


	public void close() {
		try {
			if(!socket.isClosed())
				socket.close();
		}
		catch (final IOException e) {
			logger.error("close socket failed", e);
		}
		thread.interrupt();
	}
}
