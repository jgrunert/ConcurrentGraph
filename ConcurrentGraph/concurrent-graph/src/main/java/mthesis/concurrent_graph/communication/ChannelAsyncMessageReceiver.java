package mthesis.concurrent_graph.communication;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Receives asynchronous messages on a channel from another machine. Runs a receive thread.
 *
 * @author Jonas Grunert
 *
 */
public class ChannelAsyncMessageReceiver<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	private final Logger logger;
	private final int ownId;
	private final Socket socket;
	private final InputStream reader;
	private final byte[] inBytes = new byte[Configuration.MAX_MESSAGE_SIZE];
	private final ByteBuffer inBuffer = ByteBuffer.wrap(inBytes);
	private final AbstractMachine<V, E, M, Q> inMsgHandler;
	private Thread thread;
	private volatile boolean readyForClose;
	private final VertexWorkerInterface<V, E, M, Q> worker;
	private final JobConfiguration<V, E, M, Q> jobConfig;
	private final VertexFactory<V, E, M, Q> vertexFactory;

	private final VertexMessagePool<V, E, M, Q> vertexMessagePool;


	public ChannelAsyncMessageReceiver(Socket socket, InputStream reader, int ownId,
			AbstractMachine<V, E, M, Q> inMsgHandler,
			VertexWorkerInterface<V, E, M, Q> worker, JobConfiguration<V, E, M, Q> jobConfig) {
		this.ownId = ownId;
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.reader = reader;
		this.inMsgHandler = inMsgHandler;
		this.worker = worker;
		this.jobConfig = jobConfig;
		this.vertexMessagePool = new VertexMessagePool<>(jobConfig);
		this.vertexFactory = jobConfig != null ? jobConfig.getVertexFactory() : null;
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
						reader.read(inBytes, 0, 4);
						msgContentLength = inBuffer.getInt();

						if (msgContentLength == ChannelAsyncMessageSender.ChannelCloseSignal) {
							logger.debug("Received channel close signal");
							return;
						}
						if (msgContentLength <= 0) {
							logger.error(
									"Receive error, message with non positive content length: " + msgContentLength);
							socket.close();
							return;
						}
						if (msgContentLength > Configuration.MAX_MESSAGE_SIZE) {
							logger.error("Receive error, to long message: " + msgContentLength);
							socket.close();
							return;
						}

						inBuffer.clear();
						readIndex = 0;
						while (readIndex < msgContentLength) {
							readIndex += reader.read(inBytes, readIndex, msgContentLength - readIndex);
							if (readIndex == -1) {
								logger.debug("Reader returned -1, exiting reader");
								socket.close();
								return;
							}
						}

						//						if (msgContentLength == 100) {
						//							String line = "";
						//							for (int i = 0; i < 104; i++) {
						//								line += inBuffer.array()[i] + ", ";
						//							}
						//							System.out.println(line);
						//						}

						final byte msgType = inBuffer.get();
						switch (msgType) {
							case 0:
								inMsgHandler
								.onIncomingMessage(vertexMessagePool.getPooledVertexMessage(inBuffer, 1));
								break;
							case 1:
								readIncomingMessageEnvelope(1, msgContentLength - 1);
								break;
							case 2:
								inMsgHandler.onIncomingMessage(new GetToKnowMessage(inBuffer));
								break;
							case 3:
								inMsgHandler.onIncomingMessage(
										new MoveVerticesMessage<>(inBuffer, worker, jobConfig, vertexFactory));
								break;
							case 4:
								inMsgHandler.onIncomingMessage(
										new UpdateRegisteredVerticesMessage(inBuffer));
								break;

							default:
								logger.warn("Unknown incoming message id: " + msgType);
								break;
						}
						inBuffer.clear();
					}
				}
				catch (final Throwable e) {
					System.out.println("close " + socket.isClosed() + " " + readyForClose);
					if (!readyForClose) {
						if (socket.isClosed()) logger.debug("Socket closed");
						else logger.error("receive error", e);
					}
				}
				finally {
					System.out.println("finally " + socket.isClosed() + " " + readyForClose);
					logger.debug("ChannelMessageReceiver closed: " + socket.isClosed());
					try {
						if (!socket.isClosed()) socket.close();
					}
					catch (final IOException e) {
						logger.error("close socket failed", e);
					}
				}
			}
		});
		thread.setDaemon(true);
		thread.setName("ChannelAsyncReceiver_" + ownId + "_" + connectedMachineId);
		thread.start();
	}

	private void readIncomingMessageEnvelope(int offset, int size) throws InvalidProtocolBufferException {
		final MessageEnvelope messageEnv = MessageEnvelope.parseFrom(ByteString.copyFrom(inBytes, offset, size));
		inMsgHandler.onIncomingMessage(new ProtoEnvelopeMessage(messageEnv, false));
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
