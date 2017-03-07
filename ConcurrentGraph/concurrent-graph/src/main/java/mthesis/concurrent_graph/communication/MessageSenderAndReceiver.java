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
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;


/**
 * Class to handle messaging between nodes.
 * Communicatino is based on java sockets.
 *
 * @author Jonas Grunert
 *
 */
public class MessageSenderAndReceiver<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	private final Logger logger;

	private final int ownId;
	private final Map<Integer, MachineConfig> machineConfigs;
	private final ConcurrentHashMap<Integer, ChannelAsyncMessageSender<V, E, M, Q>> channelAsyncSenders = new ConcurrentHashMap<>();
	private final List<ChannelAsyncMessageReceiver<V, E, M, Q>> channelAsyncReceivers = new LinkedList<>();
	private final AbstractMachine<V, E, M, Q> machine;
	private final VertexWorkerInterface<V, E, M, Q> workerMachine;
	private final JobConfiguration<V, E, M, Q> jobConfiguration;
	private Thread serverThread;
	private ServerSocket serverSocket;
	private boolean closingServer;

	private final LinkedList<VertexMessage<V, E, M, Q>> vertexMessagePool = new LinkedList<>();



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
		final long timeoutTime = System.currentTimeMillis() + Configuration.CONNECT_TIMEOUT;
		while (System.currentTimeMillis() <= timeoutTime &&
				!(channelAsyncReceivers.size() == (machineConfigs.size() - 1)
						&& channelAsyncSenders.size() == (machineConfigs.size() - 1))) {
			try {
				Thread.sleep(1);
			}
			catch (final InterruptedException e) {
				break;
			}
		}
		if (channelAsyncReceivers.size() == (machineConfigs.size() - 1) && channelAsyncSenders.size() == (machineConfigs.size() - 1)) {
			logger.info("Established all connections");
			return true;
		}
		else {
			logger.error("Timeout while wait for establish all connections");
			return false;
		}
	}

	public void getReadyForClose() {
		for (final ChannelAsyncMessageReceiver<V, E, M, Q> channel : channelAsyncReceivers) {
			channel.getReadyForClose();
		}
	}

	public void stop() {
		closingServer = true;
		for (final ChannelAsyncMessageSender<V, E, M, Q> channel : channelAsyncSenders.values()) {
			channel.close();
		}
		for (final ChannelAsyncMessageReceiver<V, E, M, Q> channel : channelAsyncReceivers) {
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


	/**
	 * Sends a message through the channel asynchronously, without acknowledgement
	 */
	public void sendUnicastMessageAsync(int dstMachine, ChannelMessage message) {
		final ChannelAsyncMessageSender<V, E, M, Q> ch = channelAsyncSenders.get(dstMachine);
		ch.sendMessageAsync(message);
	}

	/**
	 * Sends a message through the channel asynchronously, without acknowledgement
	 */
	public void sendMulticastMessageAsync(List<Integer> dstMachines, ChannelMessage message) {
		for (final Integer machineId : dstMachines) {
			final ChannelAsyncMessageSender<V, E, M, Q> ch = channelAsyncSenders.get(machineId);
			ch.sendMessageAsync(message);
		}
	}


	public void sendControlMessageUnicast(int dstMachine, MessageEnvelope message, boolean flush) {
		sendUnicastMessageAsync(dstMachine, new ProtoEnvelopeMessage(message, flush));
	}

	public void sendControlMessageMulticast(List<Integer> dstMachines, MessageEnvelope message, boolean flush) {
		sendMulticastMessageAsync(dstMachines, new ProtoEnvelopeMessage(message, flush));
	}


	public void sendVertexMessageUnicast(int dstMachine, int superstepNo, int srcMachine, int queryId,
			List<Pair<Integer, M>> vertexMessages) {
		if (vertexMessages.isEmpty()) return;
		sendUnicastMessageAsync(dstMachine, getPooledVertexMessage(superstepNo, ownId, false, queryId, vertexMessages));
	}

	public void sendVertexMessageBroadcast(List<Integer> otherWorkers, int superstepNo, int srcMachine,
			int queryId, List<Pair<Integer, M>> vertexMessages) {
		if (vertexMessages.isEmpty()) return;
		// Dont use message multiple times to allow free/reuse
		for (final Integer dstMachine : otherWorkers) {
			sendUnicastMessageAsync(dstMachine, getPooledVertexMessage(superstepNo, ownId, true, queryId, vertexMessages));
		}
	}

	private VertexMessage<V, E, M, Q> getPooledVertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages) {
		VertexMessage<V, E, M, Q> message;
		synchronized (vertexMessagePool) {
			message = vertexMessagePool.poll();
		}
		if (message == null) {
			message = new VertexMessage<>(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages, vertexMessagePool);
		}
		else {
			message.setup(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages);
		}
		return message;
	}

	public void freeVertexMessage(VertexMessage<V, E, M, Q> message) {
		synchronized (vertexMessagePool) {
			vertexMessagePool.add(message);
		}
	}


	public void sendGetToKnownMessage(int dstMachine, Collection<Integer> vertices, int queryId) {
		sendUnicastMessageAsync(dstMachine, new GetToKnowMessage(ownId, queryId, vertices));
	}

	public void sendMoveVerticesMessage(int dstMachine, Collection<AbstractVertex<V, E, M, Q>> vertices, int queryId,
			boolean lastSegment) {
		sendUnicastMessageAsync(dstMachine, new MoveVerticesMessage<>(ownId, queryId, vertices, lastSegment));
	}

	public void sendInvalidateRegisteredVerticesMessage(int dstMachine, Collection<Integer> vertices, int movedTo, int queryId) {
		sendUnicastMessageAsync(dstMachine, new UpdateRegisteredVerticesMessage(ownId, queryId, movedTo, vertices));
	}


	/**
	 * Flushes the async channel to a given machine
	 */
	public void flushAsyncChannel(int machineId) {
		channelAsyncSenders.get(machineId).flush();
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
			socket.setTcpNoDelay(Configuration.TCP_NODELAY);
		}
		catch (final SocketException e) {
			logger.error("set socket configs", e);
		}
		final ChannelAsyncMessageReceiver<V, E, M, Q> receiver = new ChannelAsyncMessageReceiver<>(socket, reader, ownId,
				machine, workerMachine, jobConfiguration);
		receiver.startReceiver(machineId);
		channelAsyncReceivers.add(receiver);
		final ChannelAsyncMessageSender<V, E, M, Q> sender = new ChannelAsyncMessageSender<>(socket, writer, ownId);
		sender.startSender(ownId, machineId);
		channelAsyncSenders.put(machineId, sender);
	}
}
