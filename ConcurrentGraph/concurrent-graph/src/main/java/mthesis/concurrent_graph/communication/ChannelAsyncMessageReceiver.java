package mthesis.concurrent_graph.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQuery;
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
public class ChannelAsyncMessageReceiver<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	private final Logger logger;
	private final int ownId;
	private final Socket socket;
	private final InputStream reader;
	private final OutputStream writer;
	private final byte[] inBytes = new byte[Configuration.MAX_MESSAGE_SIZE];
	private final ByteBuffer inBuffer = ByteBuffer.wrap(inBytes);
	private final AbstractMachine<V, E, M, Q> inMsgHandler;
	private Thread thread;
	private volatile boolean readyForClose;
	private final VertexWorkerInterface<V, E, M, Q> worker;
	private final JobConfiguration<V, E, M, Q> jobConfig;
	private final VertexFactory<V, E, M, Q> vertexFactory;

	public ChannelAsyncMessageReceiver(Socket socket, InputStream reader, OutputStream writer, int ownId,
			AbstractMachine<V, E, M, Q> inMsgHandler,
			VertexWorkerInterface<V, E, M, Q> worker, JobConfiguration<V, E, M, Q> jobConfig) {
		this.ownId = ownId;
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.reader = reader;
		this.writer = writer;
		this.inMsgHandler = inMsgHandler;
		this.worker = worker;
		this.jobConfig = jobConfig;
		this.vertexFactory = jobConfig != null ? jobConfig.getVertexFactory() : null;
	}


	public void startReceiver(int connectedMachineId) {
		//		Random testRd = new Random(ownId);
		readyForClose = false;
		thread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					int msgContentLength;
					int readIndex;
					while (!Thread.interrupted() && !socket.isClosed()) {
						//						if (testRd.nextDouble() < 0.01) {
						//							logger.warn("Random delay");
						//							Thread.sleep(200);
						//						}

						//						synchronized (reader)
						{
							inBuffer.clear();
							reader.read(inBytes, 0, 4);
							msgContentLength = inBuffer.getInt();
						}

						// Check for closed socket or message error
						if (msgContentLength == ChannelAsyncMessageSender.ChannelCloseSignal) {
							logger.debug("Received channel close signal");
							return;
						}
						if (msgContentLength <= 0) {
							Thread.sleep(100); // Sleep to wait wait until socket is closed when shutting down
							if (socket.isClosed()) {
								if (!readyForClose) {
									logger.info("Receive error after closed socket, message with non positive content length: "
											+ msgContentLength);
								}
							}
							else {
								if (!readyForClose) {
									logger.error("Receive error, message with non positive content length: " + msgContentLength);
								}
								socket.close();
							}
							return;
						}
						if ((msgContentLength + 4) > Configuration.MAX_MESSAGE_SIZE) {
							Thread.sleep(100); // Sleep to wait wait until socket is closed when shutting down
							if (socket.isClosed()) {
								if (!readyForClose) {
									logger.info("Receive error after closed socket, message with too long content length: "
											+ msgContentLength);
								}
							}
							else {
								if (!readyForClose) {
									logger.error("Receive error, message with too long content length: " + msgContentLength);
								}
								socket.close();
							}
							return;
						}

						//						synchronized (reader)
						{
							inBuffer.clear();
							readIndex = 0;
							while (readIndex < msgContentLength) {
								readIndex += reader.read(inBytes, readIndex, msgContentLength - readIndex);
								if (readIndex == -1) {
									logger.debug("Reader returned -1, exiting reader");
									socket.close();
									return;
								}
							} // TODO Check length correct. CHECK FOR Newline
						}

						final byte msgType = inBuffer.get();
						switch (msgType) {
							case 0:
								inMsgHandler
								.onIncomingMessage(new VertexMessage<>(inBuffer, jobConfig));
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
					}
				}
				catch (final Throwable e) {
					if (!readyForClose) {
						if (socket.isClosed()) logger.debug("Socket closed");
						else logger.error("receive error", e);
					}
				}
				finally {
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
