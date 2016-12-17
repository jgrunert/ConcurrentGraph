package mthesis.concurrent_graph.communication;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Class to handle messaging between nodes.
 * Based on Netty channels.
 * 
 * @author jonas
 *
 */
public class MessageSenderAndReceiver<M extends BaseWritable> {
	private final Logger logger;

	private final int ownId;
	private final Map<Integer, MachineConfig> machines;
	private final ConcurrentHashMap<Integer, ChannelMessageSender> channelSenders = new ConcurrentHashMap<>();
	private final List<ChannelMessageReceiver<M>> channelReceivers = new LinkedList<>();
	private final AbstractMachine<M> messageListener;
	private final BaseWritable.BaseWritableFactory<M> vertexMessageFactory;
	private Thread serverThread;
	private ServerSocket serverSocket;


	public MessageSenderAndReceiver(Map<Integer, MachineConfig> machines, int ownId,
			AbstractMachine<M> listener, BaseWritable.BaseWritableFactory<M> vertexMessageFactory) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.ownId = ownId;
		this.machines = machines;
		this.messageListener = listener;
		this.vertexMessageFactory = vertexMessageFactory;
	}

	//	public void setMessageListner(AbstractNode listener) {
	//		this.messageListener = listener;
	//	}


	public void startServer() {
		try {
			serverThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						runServer();
					}
					catch (final Exception e) {
						if(!serverSocket.isClosed())
							logger.error("runServer", e);
					}
				}
			});
			serverThread.setName("MessageServerThread_" + ownId);
			serverThread.start();
		}
		catch (final Exception e2) {
			logger.error("Starting server failed", e2);
		}
	}


	public boolean startChannels() {
		// Connect to all other machines with smaller IDs
		for(final Entry<Integer, MachineConfig> machine : machines.entrySet()) {
			if(machine.getKey() < ownId) {
				try {
					connectToMachine(machine.getValue().HostName, machine.getValue().MessagePort, machine.getKey());
				} catch (final Exception e) {
					logger.error("Exception at connectToMachine " + machine.getKey(), e);
					return false;
				}
			}
		}
		return true;
	}

	public boolean waitUntilConnected() {
		final long timeoutTime = System.currentTimeMillis() + Settings.CONNECT_TIMEOUT;
		while(System.currentTimeMillis() <= timeoutTime &&
				!(channelReceivers.size() == (machines.size() - 1) && channelSenders.size() == (machines.size() - 1))) {
			try {
				Thread.sleep(1);
			}
			catch (final InterruptedException e) {
				break;
			}
		}
		if(channelReceivers.size() == (machines.size() - 1) && channelSenders.size() == (machines.size() - 1)) {
			logger.info("Established all connections");
			return true;
		}
		else {
			logger.error("Timeout while wait for establish all connections");
			return false;
		}
	}

	public void stop() {
		for(final ChannelMessageSender channel : channelSenders.values()) {
			channel.close();
		}
		for(final ChannelMessageReceiver<M> channel : channelReceivers) {
			channel.close();
		}
		try {
			serverSocket.close();
		}
		catch (final IOException e) {
			logger.error("closing server failed", e);
		}
		serverThread.interrupt();
	}


	public void sendControlMessageUnicast(int dstId, MessageEnvelope message, boolean flush) {
		final ChannelMessageSender ch = channelSenders.get(dstId);
		ch.sendMessageEnvelope(message, flush);
	}
	public void sendControlMessageMulticast(List<Integer> dstIds, MessageEnvelope message, boolean flush) {
		for(final Integer machineId : dstIds) {
			sendControlMessageUnicast(machineId, message, flush);
		}
	}

	public void sendVertexMessageUnicast(int dstMachine, BaseWritable message, int superstepNo, int srcVertex, int dstVertex, boolean flush) {
		final ChannelMessageSender ch = channelSenders.get(dstMachine);
		ch.sendVertexMessage(message, superstepNo, ownId, srcVertex, dstVertex, flush);
	}
	public void sendVertexMessageMulticast(List<Integer> dstIds, BaseWritable message, int superstepNo, int srcVertex, int dstVertex, boolean flush) {
		for(final Integer machineId : dstIds) {
			sendVertexMessageUnicast(machineId, message, superstepNo, srcVertex, dstVertex, flush);
		}
	}


	public void onIncomingControlMessage(ControlMessage message) {
		messageListener.onIncomingControlMessage(message);
	}

	public void onIncomingVertexMessage(int msgSuperstepNo, int srcMachine, int srcVertex, int dstVertex, M messageContent) {
		messageListener.onIncomingVertexMessage(msgSuperstepNo, srcMachine, srcVertex, dstVertex, messageContent);
	}


	private void connectToMachine(String host, int port, int machineId) throws Exception {
		final Socket socket = new Socket(host, port);
		logger.debug("Connected to: " + host + ":" + port + " for machine channel " + machineId);

		final DataOutputStream writer = new DataOutputStream(socket.getOutputStream());
		final DataInputStream reader = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		writer.writeInt(ownId);
		startConnection(machineId, socket, writer, reader);
		logger.debug("Handshaked and established connection channel: " + machineId + " <- " + ownId);
	}

	private void runServer() throws Exception {
		final int port = machines.get(ownId).MessagePort;
		serverSocket = new ServerSocket(port);
		logger.info("Started connection server");

		try{
			while(!Thread.interrupted() && !serverSocket.isClosed()) {
				final Socket clientSocket = serverSocket.accept();

				logger.debug("Accepted connection: " + clientSocket);
				final DataInputStream reader = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
				final DataOutputStream writer = new DataOutputStream(clientSocket.getOutputStream());

				final int connectedMachineId = reader.readInt();
				startConnection(connectedMachineId, clientSocket, writer, reader);
				logger.debug("Handshaked and established connection channel: " + connectedMachineId + " -> " + ownId + " " + clientSocket);
			}
		}
		finally{
			serverSocket.close();
			logger.info("Closed connection server");
		}
	}


	private void startConnection(int machineId, final Socket socket, final DataOutputStream writer,
			final DataInputStream reader) {
		final ChannelMessageReceiver<M> receiver = new ChannelMessageReceiver<>(socket, reader, ownId, this, vertexMessageFactory);
		receiver.startReceiver(machineId);
		channelReceivers.add(receiver);
		final ChannelMessageSender sender = new ChannelMessageSender(socket, writer, ownId);
		sender.startSender(ownId, machineId);
		channelSenders.put(machineId, sender);
	}
}
