package mthesis.concurrent_graph.communication;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;


/**
 * Receives messages on a channel from another machine. Runs a receive thread.
 * 
 * @author Jonas Gruenrt
 *
 */
public class ChannelMessageReceiver {
	private final Logger logger;
	private final int ownId;
	private final Socket socket;
	private final DataInputStream reader;
	private final byte[] buffer = new byte[Settings.MAX_MESSAGE_SIZE];
	private Thread thread;

	public ChannelMessageReceiver(Socket socket, DataInputStream reader, int ownId) {
		this.ownId = ownId;
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.socket = socket;
		this.reader = reader;
	}

	int correctRecM = 0;
	int correctRecBts = 0;
	public void startReceiver(int connectedMachineId, MessageSenderAndReceiver inMsgHandler) {
		thread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					int msgLength, readIndex;
					while(!Thread.interrupted() && !socket.isClosed()) {
						msgLength = reader.readShort();

						readIndex = 0;
						while(readIndex < msgLength) {
							readIndex += reader.read(buffer, readIndex, msgLength - readIndex);
						}

						final MessageEnvelope message = MessageEnvelope.parseFrom(ByteString.copyFrom(buffer, 0, msgLength));
						if(message.hasVertexMessage())
							inMsgHandler.onIncomingVertexMessage(message.getVertexMessage());
						else if(message.hasControlMessage())
							inMsgHandler.onIncomingControlMessage(message.getControlMessage());

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

	public void close() {
		try {
			socket.close();
		}
		catch (final IOException e) {
			logger.error("close socket failed", e);
		}
		thread.interrupt();
	}
}
