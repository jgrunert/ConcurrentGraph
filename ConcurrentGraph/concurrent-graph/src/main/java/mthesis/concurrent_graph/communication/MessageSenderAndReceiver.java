package mthesis.concurrent_graph.communication;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Class to handle messaging between nodes.
 * Based on java sockets.
 *
 * @author Jonas Grunert
 *
 */
public class MessageSenderAndReceiver<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	private final Logger logger;

	private final int ownId;
	private final Map<Integer, MachineConfig> machineConfigs;
	private final ConcurrentHashMap<Integer, ChannelMessageSender<V, E, M, Q>> channelSenders = new ConcurrentHashMap<>();
	private final List<ChannelMessageReceiver<V, E, M, Q>> channelReceivers = new LinkedList<>();
	private final AbstractMachine<V, E, M, Q> machine;
	private final VertexWorkerInterface<V, E, M, Q> workerMachine;
	private final JobConfiguration<V, E, M, Q> jobConfiguration;
	private Thread serverThread;
	private ServerSocket serverSocket;
	private boolean closingServer;



	public MessageSenderAndReceiver(Map<Integer, MachineConfig> machines, int ownId,
			VertexWorkerInterface<V, E, M, Q> workerMachine, AbstractMachine<V, E, M, Q> machine,
			JobConfiguration<V, E, M, Q> jobConfiguration) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		this.ownId = ownId;
		this.machineConfigs = machines;
		this.machine = machine;
		this.workerMachine = workerMachine;
		this.jobConfiguration = jobConfiguration;
	}

	//	public void setMessageListner(AbstractNode listener) {
	//		this.messageListener = listener;
	//	}


	public void startServer() {
		try {
			closingServer = false;
			serverThread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						runServer();
					}
					catch (final Exception e) {
						if (!closingServer)
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
		for (final Entry<Integer, MachineConfig> machine : machineConfigs.entrySet()) {
			if (machine.getKey() < ownId) {
				try {
					connectToMachine(machine.getValue().HostName, machine.getValue().MessagePort, machine.getKey());
				}
				catch (final Exception e) {
					logger.error("Exception at connectToMachine " + machine.getKey(), e);
					return false;
				}
			}
		}
		return true;
	}

	public boolean waitUntilConnected() {
		final long timeoutTime = System.currentTimeMillis() + Settings.CONNECT_TIMEOUT;
		while (System.currentTimeMillis() <= timeoutTime &&
				!(channelReceivers.size() == (machineConfigs.size() - 1) && channelSenders.size() == (machineConfigs.size() - 1))) {
			try {
				Thread.sleep(1);
			}
			catch (final InterruptedException e) {
				break;
			}
		}
		if (channelReceivers.size() == (machineConfigs.size() - 1) && channelSenders.size() == (machineConfigs.size() - 1)) {
			logger.info("Established all connections");
			return true;
		}
		else {
			logger.error("Timeout while wait for establish all connections");
			return false;
		}
	}

	public void getReadyForClose() {
		for (final ChannelMessageReceiver<V, E, M, Q> channel : channelReceivers) {
			channel.getReadyForClose();
		}
	}

	public void stop() {
		closingServer = true;
		for (final ChannelMessageSender<V, E, M, Q> channel : channelSenders.values()) {
			channel.close();
		}
		for (final ChannelMessageReceiver<V, E, M, Q> channel : channelReceivers) {
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
		final ChannelMessageSender<V, E, M, Q> ch = channelSenders.get(dstId);
		ch.sendMessageEnvelope(message, flush);
	}

	public void sendControlMessageMulticast(List<Integer> dstIds, MessageEnvelope message, boolean flush) {
		for (final Integer machineId : dstIds) {
			sendControlMessageUnicast(machineId, message, flush);
		}
	}

	public void sendVertexMessageUnicast(int dstMachine, int superstepNo, int srcMachine, int queryId,
			List<Pair<Integer, M>> vertexMessages) {
		if (vertexMessages.isEmpty()) return;
		final ChannelMessageSender<V, E, M, Q> ch = channelSenders.get(dstMachine);
		ch.sendVertexMessage(superstepNo, srcMachine, false, queryId, vertexMessages);
	}

	public void sendVertexMessageBroadcast(List<Integer> otherWorkerIds, int superstepNo, int srcMachine,
			int queryId, List<Pair<Integer, M>> vertexMessages) {
		if (vertexMessages.isEmpty()) return;
		for (final Integer machineId : otherWorkerIds) {
			final ChannelMessageSender<V, E, M, Q> ch = channelSenders.get(machineId);
			ch.sendVertexMessage(superstepNo, srcMachine, true, queryId, vertexMessages);
		}
	}

	public void sendGetToKnownMessage(int dstMachine, Collection<Integer> vertices, int queryId) {
		final ChannelMessageSender<V, E, M, Q> ch = channelSenders.get(dstMachine);
		ch.sendGetToKnownMessage(ownId, vertices, queryId);
	}

	public void sendMoveVerticesMessage(int dstMachine, Collection<AbstractVertex<V, E, M, Q>> vertices, int queryId, boolean lastSegment) {
		final ChannelMessageSender<V, E, M, Q> ch = channelSenders.get(dstMachine);
		ch.sendMoveVerticesMessage(ownId, vertices, queryId, lastSegment);
	}

	public void sendInvalidateRegisteredVerticesMessage(int dstMachine, Collection<Integer> vertices, int queryId) {
		final ChannelMessageSender<V, E, M, Q> ch = channelSenders.get(dstMachine);
		ch.sendInvalidateRegisteredVerticesMessage(ownId, vertices, queryId);
	}

	public void flushChannel(int machineId) {
		channelSenders.get(machineId).flush();
	}



	private void connectToMachine(String host, int port, int machineId) throws Exception {
		final Socket socket = new Socket(host, port);
		logger.debug("Connected to: " + host + ":" + port + " for machine channel " + machineId);

		final OutputStream writer = socket.getOutputStream();
		final InputStream reader = new BufferedInputStream(socket.getInputStream());
		new DataOutputStream(writer).writeInt(ownId);
		startConnection(machineId, socket, writer, reader);
		logger.debug("Handshaked and established connection channel: " + machineId + " <- " + ownId);
	}

	private void runServer() throws Exception {
		final int port = machineConfigs.get(ownId).MessagePort;
		serverSocket = new ServerSocket(port);
		logger.info("Started connection server");

		try {
			while (!Thread.interrupted() && !serverSocket.isClosed()) {
				final Socket clientSocket = serverSocket.accept();

				logger.debug("Accepted connection: " + clientSocket);
				final InputStream reader = new BufferedInputStream(clientSocket.getInputStream());
				final OutputStream writer = clientSocket.getOutputStream();

				final int connectedMachineId = new DataInputStream(reader).readInt();
				startConnection(connectedMachineId, clientSocket, writer, reader);
				logger.debug("Handshaked and established connection channel: " + connectedMachineId + " -> " + ownId + " " + clientSocket);
			}
		}
		finally {
			serverSocket.close();
			logger.info("Closed connection server");
		}
	}


	private void startConnection(int machineId, final Socket socket, final OutputStream writer,
			final InputStream reader) {
		try {
			socket.setTcpNoDelay(Settings.TCP_NODELAY);
		}
		catch (final SocketException e) {
			logger.error("set socket configs", e);
		}
		final ChannelMessageReceiver<V, E, M, Q> receiver = new ChannelMessageReceiver<>(socket, reader, ownId,
				machine, workerMachine, jobConfiguration);
		receiver.startReceiver(machineId);
		channelReceivers.add(receiver);
		final ChannelMessageSender<V, E, M, Q> sender = new ChannelMessageSender<>(socket, writer, ownId);
		sender.startSender(ownId, machineId);
		channelSenders.put(machineId, sender);
	}
}
