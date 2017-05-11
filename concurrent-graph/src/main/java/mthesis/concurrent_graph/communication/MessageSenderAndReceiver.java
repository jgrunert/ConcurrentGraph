package mthesis.concurrent_graph.communication;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQuery;
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
public class MessageSenderAndReceiver<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	private final Logger logger;

	private static final int ConnectRetryWaitTime = 2000;

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
					finally {
						logger.debug("Finished connection server, closing: " + closingServer);
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
			logger.error("Timeout while wait for establish all connections, connected receivers "
					+ channelAsyncReceivers.size() + "/" + (machineConfigs.size() - 1) + " senders "
					+ channelAsyncSenders.size() + "/" + (machineConfigs.size() - 1));
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
	public int sendUnicastMessageSync(int dstMachine, ChannelMessage message) {
		final ChannelAsyncMessageSender<V, E, M, Q> ch = channelAsyncSenders.get(dstMachine);
		return ch.sendMessageSync(message);
	}

	/**
	 * Sends a message through the channel asynchronously, without acknowledgement
	 */
	public int sendMulticastMessageSync(List<Integer> dstMachines, ChannelMessage message) {
		int length = 0;
		for (final Integer machineId : dstMachines) {
			final ChannelAsyncMessageSender<V, E, M, Q> ch = channelAsyncSenders.get(machineId);
			length += ch.sendMessageSync(message);
		}
		return length;
	}


	public int sendControlMessageUnicast(int dstMachine, MessageEnvelope message, boolean flush) {
		return sendUnicastMessageSync(dstMachine, new ProtoEnvelopeMessage(message, flush));
	}

	public int sendControlMessageMulticast(List<Integer> dstMachines, MessageEnvelope message, boolean flush) {
		return sendMulticastMessageSync(dstMachines, new ProtoEnvelopeMessage(message, flush));
	}


	public void sendVertexMessageUnicast(int dstMachine, int superstepNo, int srcMachine, int queryId,
			boolean fromLocalMode,
			List<Pair<Integer, M>> vertexMessages) {
		if (vertexMessages.isEmpty()) return;
		sendUnicastMessageSync(dstMachine,
				//				new VertexMessage<>(superstepNo, ownId, false, queryId, vertexMessages, 1));
				new VertexMessage<>(superstepNo, ownId, false, queryId, fromLocalMode, vertexMessages));
	}

	public void sendVertexMessageBroadcast(List<Integer> otherWorkers, int superstepNo, int srcMachine,
			int queryId, boolean fromLocalMode, List<Pair<Integer, M>> vertexMessages) {
		if (vertexMessages.isEmpty()) return;
		// Dont use message multiple times to allow free/reuse
		//		for (final Integer dstMachine : otherWorkers) {
		//			sendUnicastMessageAsync(dstMachine,
		//					vertexMessagePool.getPooledVertexMessage(superstepNo, ownId, true, queryId, vertexMessages, 1));
		//		}
		sendMulticastMessageSync(otherWorkers,
				//				new VertexMessage<>(superstepNo, ownId, true, queryId, vertexMessages, otherWorkers.size()));
				new VertexMessage<>(superstepNo, ownId, true, queryId, fromLocalMode, vertexMessages));
	}


	public void sendGetToKnownMessage(int dstMachine, Collection<Integer> vertices, int queryId) {
		sendUnicastMessageSync(dstMachine, new GetToKnowMessage(ownId, queryId, vertices));
	}

	public void sendMoveVerticesMessage(int dstMachine, Set<Integer> chunkQueries, Set<Integer> vertexQueries,
			List<AbstractVertex<V, E, M, Q>> vertices,
			boolean lastSegment) {
		sendUnicastMessageSync(dstMachine, new MoveVerticesMessage<>(ownId, chunkQueries, vertexQueries, vertices, lastSegment));
	}

	public void sendInvalidateRegisteredVerticesMessage(int dstMachine, Collection<Integer> vertices, int movedTo) {
		sendUnicastMessageSync(dstMachine, new UpdateRegisteredVerticesMessage(ownId, movedTo, vertices));
	}


	/**
	 * Flushes the async channel to a given machine
	 */
	public void flushAsyncChannel(int machineId) {
		channelAsyncSenders.get(machineId).flush();
	}



	private void connectToMachine(String host, int port, int machineId) throws Exception {

		{
			Socket socket = connectSocket(host, port, machineId);
			logger.debug("Connected to: " + host + ":" + port + " for receive channel " + machineId);
			final InputStream reader = new BufferedInputStream(socket.getInputStream());
			//final InputStream reader = socket.getInputStream();
			final OutputStream writer = socket.getOutputStream();
			DataOutputStream outWriter = new DataOutputStream(writer);
			outWriter.writeBoolean(true);
			outWriter.writeInt(ownId);
			startSenderConnection(machineId, socket, writer, reader);
			logger.debug("Handshaked and established receiver connection channel: " + machineId + " <- " + ownId);
		}
		{
			Socket socket = connectSocket(host, port, machineId);
			logger.debug("Connected to: " + host + ":" + port + " for send channel " + machineId);
			final InputStream reader = new BufferedInputStream(socket.getInputStream());
			//final InputStream reader = socket.getInputStream();
			final OutputStream writer = socket.getOutputStream();
			DataOutputStream outWriter = new DataOutputStream(writer);
			outWriter.writeBoolean(false);
			outWriter.writeInt(ownId);
			startReceiverConnection(machineId, socket, writer, reader);
			logger.debug("Handshaked and established receiver sender channel: " + machineId + " <- " + ownId);
		}
	}

	private Socket connectSocket(String host, int port, int machineId) throws Exception {
		int tries = 0;
		long connectStartTime = System.currentTimeMillis();
		while (true) {
			try {
				return new Socket(host, port);
			}
			catch (ConnectException e) {
				tries++;
				logger.debug("Connect failed to: " + host + ":" + port + " for machine channel " + machineId + " try "
						+ tries);
				if ((System.currentTimeMillis() - connectStartTime) < Configuration.CONNECT_TIMEOUT)
					Thread.sleep(ConnectRetryWaitTime);
				else {
					logger.error("Giving up connecting  to: " + host + ":" + port + " for machine channel " + machineId,
							e);
					throw e;
				}
			}
		}
	}

	private void runServer() throws Exception {
		MachineConfig config = machineConfigs.get(ownId);
		if (config == null) {
			logger.error("No machine config for " + ownId);
			return;
		}
		final int port = config.MessagePort;
		try {
			serverSocket = new ServerSocket(port);
		}
		catch (Throwable e) {
			logger.error("Failed to start ServerSocket on port " + port, e);
		}
		logger.info("Started connection server");

		boolean interrupted = false;
		boolean closedTest = false;
		try {
			while (!(interrupted = Thread.interrupted()) && !(closedTest = serverSocket.isClosed())) {
				final Socket clientSocket = serverSocket.accept();

				logger.debug("Accepted connection: " + clientSocket);
				final InputStream reader = new BufferedInputStream(clientSocket.getInputStream());
				//final InputStream reader = clientSocket.getInputStream();
				final OutputStream writer = clientSocket.getOutputStream();

				DataInputStream inStream = new DataInputStream(reader);
				final boolean clientSendConnection = inStream.readBoolean();
				final int connectedMachineId = inStream.readInt();
				if (clientSendConnection)
					startReceiverConnection(connectedMachineId, clientSocket, writer, reader);
				else startSenderConnection(connectedMachineId, clientSocket, writer, reader);
				logger.debug("Handshaked and established connection channel: " + connectedMachineId + " -> " + ownId + " " + clientSocket);
			}
			logger.debug("connection server loop finished");
		}
		catch (SocketException e) {
			if (closingServer) logger.debug("Socket closed by exception at runServer");
			else logger.error("Error at runServer", e);
		}
		catch (Throwable e) {
			logger.error("Error at runServer", e);
		}
		finally {
			logger.info(
					"Closed connection server, closed: " + serverSocket.isClosed() + "/" + closedTest + " interrupted: "
							+ interrupted);
			serverSocket.close();
		}
	}


	private void startReceiverConnection(int machineId, final Socket socket, final OutputStream writer,
			final InputStream reader) {
		try {
			socket.setTcpNoDelay(Configuration.TCP_NODELAY);
		}
		catch (final SocketException e) {
			logger.error("set socket configs", e);
		}
		final ChannelAsyncMessageReceiver<V, E, M, Q> receiver = new ChannelAsyncMessageReceiver<>(socket, reader,
				writer, ownId, machine, workerMachine, jobConfiguration);
		receiver.startReceiver(machineId);
		channelAsyncReceivers.add(receiver);
	}

	private void startSenderConnection(int machineId, final Socket socket, final OutputStream writer,
			final InputStream reader) {
		try {
			socket.setTcpNoDelay(Configuration.TCP_NODELAY);
		}
		catch (final SocketException e) {
			logger.error("set socket configs", e);
		}
		final ChannelAsyncMessageSender<V, E, M, Q> sender = new ChannelAsyncMessageSender<>(socket, reader, writer,
				ownId);
		sender.startSender(ownId, machineId);
		channelAsyncSenders.put(machineId, sender);
	}
}
