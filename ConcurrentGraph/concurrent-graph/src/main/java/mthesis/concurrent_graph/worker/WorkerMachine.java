package mthesis.concurrent_graph.worker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.GetToKnowMessage;
import mthesis.concurrent_graph.communication.Messages;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;
import mthesis.concurrent_graph.communication.MoveVerticesMessage;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.communication.UpdateRegisteredVerticesMessage;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.communication.VertexMessageBucket;
import mthesis.concurrent_graph.util.MiscUtil;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

/**
 * Concurrent graph processing worker main class.
 * Processes active queries. Receives messages and processes the message queue.
 *
 * @author Jonas Grunert
 *
 * @param <V>
 *            Vertex type
 * @param <E>
 *            Edge type
 * @param <M>
 *            Vertex message type
 * @param <Q>
 *            Global query values type
 */
public class WorkerMachine<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery>
extends AbstractMachine<V, E, M, Q> implements VertexWorkerInterface<V, E, M, Q> {

	private final List<Integer> otherWorkerIds;
	private final int masterId;
	private final String outputDir;
	private volatile boolean started = false;
	private boolean stopRequested = false;
	private List<String> assignedPartitions;

	private final JobConfiguration<V, E, M, Q> jobConfig;
	private final BaseVertexInputReader<V, E, M, Q> vertexReader;

	private final Int2ObjectMap<AbstractVertex<V, E, M, Q>> localVertices = new Int2ObjectOpenHashMap<>();

	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Global, aggregated QueryGlobalValues. Aggregated and sent by master
	private final Map<Integer, WorkerQuery<V, E, M, Q>> activeQueries = new HashMap<>();
	// Barrier messages that arrived before a became was active
	private final Map<Integer, List<ControlMessage>> postponedBarrierMessages = new HashMap<>();

	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	// Buckets to collect messages to send
	private final VertexMessageBucket<M> vertexMessageBroadcastBucket = new VertexMessageBucket<>();
	private final Map<Integer, VertexMessageBucket<M>> vertexMessageMachineBuckets = new HashMap<>();

	private final ConcurrentLinkedQueue<ChannelMessage> receivedMessages = new ConcurrentLinkedQueue<>();

	// Most recent calculated query intersections
	private Map<Integer, Map<Integer, Integer>> currentQueryIntersects = new HashMap<>();

	// Global barrier coordination/control
	private boolean globalBarrierRequested = false;
	private final Set<Integer> globalBarrierStartWaitSet = new HashSet<>();
	private final Set<Integer> globalBarrierStartPrematureSet = new HashSet<>();
	private final Set<Integer> globalBarrierFinishWaitSet = new HashSet<>();
	private final Set<Integer> globalBarrierFinishPrematureSet = new HashSet<>();
	// Global barrier commands to perform while barrier
	private List<Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage> globalBarrierSendVerts;
	private Set<Pair<Integer, Integer>> globalBarrierRecvVerts;

	// Worker stats
	private long masterStartTime;
	private long workerStatsLastSample = System.currentTimeMillis();
	// Current worker stats
	private WorkerStats workerStats = new WorkerStats();
	// Samples that are finished but not sent to master yet
	private List<WorkerStatSample> workerStatsSamplesToSend = new ArrayList<>();
	private PrintWriter allVertexStatsFile = null;
	private PrintWriter actVertexStatsFile = null;

	// Watchdog
	private long lastWatchdogSignal;
	private static boolean WatchdogEnabled = true;



	public WorkerMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, int masterId, String outputDir,
			JobConfiguration<V, E, M, Q> jobConfig,
			BaseVertexInputReader<V, E, M, Q> vertexReader) {
		super(machines, ownId, jobConfig);
		this.masterId = masterId;
		this.otherWorkerIds = workerIds.stream().filter(p -> p != ownId).collect(Collectors.toList());
		for (final Integer workerId : otherWorkerIds) {
			vertexMessageMachineBuckets.put(workerId, new VertexMessageBucket<>());
		}
		vertexValueFactory = jobConfig.getVertexValueFactory();
		globalValueFactory = jobConfig.getGlobalValuesFactory();

		this.outputDir = outputDir;
		this.jobConfig = jobConfig;
		this.vertexReader = vertexReader;
	}

	private void loadVertices(List<String> partitions) {
		List<AbstractVertex<V, E, M, Q>> localVerticesList = vertexReader.getVertices(partitions, jobConfig, this);
		for (final AbstractVertex<V, E, M, Q> vertex : localVerticesList) {
			localVertices.put(vertex.ID, vertex);
		}
	}

	@Override
	public void stop() {
		if (allVertexStatsFile != null) allVertexStatsFile.close();
		if (actVertexStatsFile != null) actVertexStatsFile.close();

		super.stop();
	}



	// #################### Query start/finish #################### //
	private void startQuery(Q query) {
		if (activeQueries.containsKey(query.QueryId))
			throw new RuntimeException("Thready with this ID already active: " + query.QueryId);
		WorkerQuery<V, E, M, Q> activeQuery = new WorkerQuery<>(query, globalValueFactory, localVertices.keySet());
		activeQuery.BarrierSyncWaitSet.addAll(otherWorkerIds);

		activeQueries.put(query.QueryId, activeQuery);

		// Handle postponed barrier messages if there are any
		List<ControlMessage> postponedBarrierMsgs = postponedBarrierMessages.get(query.QueryId);
		if (postponedBarrierMsgs != null) {
			for (ControlMessage message : postponedBarrierMsgs) {
				handleQuerySuperstepBarrierMsg(message, activeQuery);
			}
		}

		logger.info("Worker started query " + query.QueryId);
	}

	private void finishQuery(WorkerQuery<V, E, M, Q> activeQuery) {
		new VertexTextOutputWriter<V, E, M, Q>().writeOutput(
				outputDir + File.separator + activeQuery.Query.QueryId + File.separator + ownId + ".txt", localVertices.values(),
				activeQuery.Query.QueryId);
		sendMasterQueryFinishedMessage(activeQuery);
		for (final AbstractVertex<V, E, M, Q> vertex : localVertices.values()) {
			vertex.finishQuery(activeQuery.Query.QueryId);
		}
		logger.info("Worker finished query " + activeQuery.Query.QueryId);
		activeQueries.remove(activeQuery.Query.QueryId);
	}



	// #################### Running #################### //
	@Override
	public void run() {
		logger.info("Starting run worker node " + ownId);

		boolean interrupted = false;
		try {
			while (!started) {
				Thread.sleep(1);
				handleReceivedMessages();
			}

			// Initialize, load assigned partitions
			loadVertices(assignedPartitions);
			logger.debug("Worker loaded partitions.");
			sendMasterInitialized();

			// Start watchdog
			if (Configuration.WORKER_WATCHDOG_TIME > 0) {
				Thread watchdogThread = new Thread(new Runnable() {

					@Override
					public void run() {
						logger.debug("Watchdog started");
						while (!Thread.interrupted()) {
							if (WatchdogEnabled && (System.currentTimeMillis()
									- lastWatchdogSignal) > Configuration.WORKER_WATCHDOG_TIME) {
								logger.warn("Watchdog triggered");
								try {
									Thread.sleep(1000);
								}
								catch (InterruptedException e) {
									logger.warn("Watchdog interrupted", e);
									break;
								}
								System.exit(1);
							}
							try {
								Thread.sleep(Configuration.WORKER_WATCHDOG_TIME);
							}
							catch (InterruptedException e) {
								logger.warn("Watchdog interrupted", e);
								break;
							}
						}
					}
				});
				watchdogThread.setDaemon(true);
				lastWatchdogSignal = System.currentTimeMillis();
				logger.debug("Starting watchdog");
				watchdogThread.start();
			}

			logger.info("Starting worker execution");

			// Execution loop
			final List<WorkerQuery<V, E, M, Q>> activeQueriesThisStep = new ArrayList<>();
			while (!stopRequested && !(interrupted = Thread.interrupted())) {

				// ++++++++++ Wait for active queries ++++++++++
				long startTime = System.nanoTime();
				while (activeQueries.isEmpty()) {
					handleReceivedMessages();
				}
				workerStats.IdleTime += (System.nanoTime() - startTime);


				// ++++++++++ Wait for queries ready to compute ++++++++++
				startTime = System.nanoTime();
				activeQueriesThisStep.clear();
				while (!stopRequested && activeQueriesThisStep.isEmpty() && !globalBarrierRequested) {
					handleReceivedMessages();

					for (WorkerQuery<V, E, M, Q> activeQuery : activeQueries.values()) {
						if (activeQuery.getMasterStartedSuperstep() == activeQuery.getLastFinishedComputeSuperstep()
								+ 1)
							activeQueriesThisStep.add(activeQuery);
					}
					if (!activeQueriesThisStep.isEmpty() || globalBarrierRequested)
						break;
					if (Configuration.WORKER_SLEEP_QUERY_WAIT > 0)
						Thread.sleep(Configuration.WORKER_SLEEP_QUERY_WAIT);
					if ((System.nanoTime() - startTime) > 10000000000L) {// Warn after 10s
						logger.warn("Waiting long time for active queries");
						Thread.sleep(2000);
					}
				}
				workerStats.QueryWaitTime += (System.nanoTime() - startTime);


				// ++++++++++ Global barrier if requested and no more outstanding queries ++++++++++
				if (globalBarrierRequested && activeQueriesThisStep.isEmpty()) { // TODO Wait until all queries ready?
					// Start barrier, notify other workers
					logger.debug("Barrier started, waiting for other workers to start");
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Started(ownId),
							true);
					// --- Handle all messages before barrier ---
					handleReceivedMessages(); // TODO Necessary?

					// --- Wait for other workers barriers ---
					startTime = System.nanoTime();
					while (!globalBarrierStartWaitSet.isEmpty()) {
						handleReceivedMessages();
					}
					long barrierStartWaitTime = System.nanoTime() - startTime;

					// Checks
					for (WorkerQuery<V, E, M, Q> query : activeQueries.values()) {
						// TODO
						if (query.getMasterStartedSuperstep() != query.getLastFinishedComputeSuperstep()) {
							logger.warn("Query " + query.QueryId + " is not ready for global barrier, barrier superstep: "
									+ query.getMasterStartedSuperstep() + " " + query.getLastFinishedComputeSuperstep());
						}
						if (!query.ActiveVerticesNext.isEmpty()) {
							logger.warn("Query " + query.QueryId + " is not ready for global barrier, has ActiveVerticesNext: "
									+ query.ActiveVerticesNext.size());
						}
					}

					// --- Barrier tasks ---
					startTime = System.nanoTime();
					// Send vertices
					for (SendQueryVerticesMessage sendVert : globalBarrierSendVerts) {
						sendQueryVerticesToMove(sendVert.getQueryId(), sendVert.getMoveToMachine(),
								sendVert.getMaxMoveCount());
					}
					// Receive vertices
					while (!globalBarrierRecvVerts.isEmpty()) {
						handleReceivedMessages();
					}
					workerStats.BarrierVertexMoveTime += (System.nanoTime() - startTime);


					// --- Finish barrier, notify other workers and master ---
					logger.debug("Global barrier tasks done, waiting for other workers to finish");
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Finished(ownId), true);
					messaging.sendControlMessageUnicast(masterId,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Finished(ownId), true);
					startTime = System.nanoTime();
					while (!globalBarrierFinishWaitSet.isEmpty()) {
						handleReceivedMessages();
					}
					long barrierFinishWaitTime = System.nanoTime() - startTime;

					logger.debug("Global barrier finished on worker " + ownId);
					workerStats.BarrierStartWaitTime += barrierStartWaitTime;
					workerStats.BarrierFinishWaitTime += barrierFinishWaitTime;
					globalBarrierRequested = false;
				}


				// ++++++++++ Compute active and ready queries ++++++++++
				for (WorkerQuery<V, E, M, Q> activeQuery : activeQueriesThisStep) {
					int queryId = activeQuery.Query.QueryId;
					int superstepNo = activeQuery.getMasterStartedSuperstep();
					logger.trace("Worker start superstep " + queryId + ":" + superstepNo);
					boolean allVerticesActivate = activeQuery.QueryLocal.onWorkerSuperstepStart(superstepNo);

					// Next superstep. Compute and Messaging (done by vertices)
					logger.trace("Worker start query " + queryId + " superstep compute " + superstepNo);

					// First frame: Call all vertices, second frame only active vertices
					startTime = System.nanoTime();
					if (superstepNo >= 0) {
						if (allVerticesActivate) {
							for (final AbstractVertex<V, E, M, Q> vertex : localVertices.values()) {
								vertex.superstep(superstepNo, activeQuery, true);
							}
						}
						else {
							for (AbstractVertex<V, E, M, Q> vertex : activeQuery.ActiveVerticesThis.values()) {
								vertex.superstep(superstepNo, activeQuery, false);
							}
						}
					}
					long computeTime = System.nanoTime() - startTime;
					activeQuery.QueryLocal.Stats.ComputeTime += computeTime;


					// Send barrier sync with other workers
					if (!otherWorkerIds.isEmpty()) {
						flushVertexMessages(activeQuery);
						sendWorkersSuperstepFinished(activeQuery);
					}

					activeQuery.onFinishedSuperstepCompute(superstepNo);
					logger.trace("Worker finished compute " + queryId + ":" + superstepNo);

					// Notify master if compute and barrier sync finished
					if (activeQuery.isNextSuperstepLocallyReady()) {
						superstepLocalFinishNotifyMaster(activeQuery);
					}
				}


				// ++++++++++ Worker stats ++++++++++
				if ((System.currentTimeMillis() - workerStatsLastSample) >= Configuration.WORKER_STATS_SAMPLING_INTERVAL) {
					sampleWorkerStats();
					workerStatsLastSample = System.currentTimeMillis();
				}
			}

			logger.info("Finished worker execution");
		}
		catch (final InterruptedException e) {
			if (!stopRequested)
				logger.info("worker interrupted");
			return;
		}
		catch (final Throwable e) {
			logger.error("Exception at worker run", e);
		}
		finally {
			logger.info("Worker closing, interrupted: " + interrupted);
			messaging.getReadyForClose();
			if (!stopRequested) {
				try {
					Thread.sleep(200);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
				stop();
			}
		}
	}



	// #################### Handle incoming messages #################### //
	@Override
	public void onIncomingMessage(ChannelMessage message) {
		receivedMessages.add(message);
	}

	/**
	 * Handles all messages in received queues.
	 */
	@SuppressWarnings("unchecked")
	private void handleReceivedMessages() {
		long startTime = System.nanoTime();
		ChannelMessage message;
		while ((message = receivedMessages.poll()) != null) {
			switch (message.getTypeCode()) {
				case 0:
					handleVertexMessage((VertexMessage<V, E, M, Q>) message);
					break;
				case 1:
					ProtoEnvelopeMessage msgEnvelope = ((ProtoEnvelopeMessage) message);
					if (msgEnvelope.message.hasControlMessage()) {
						handleControlMessage(msgEnvelope);
					}
					break;
				case 2:
					handleGetToKnowMessage((GetToKnowMessage) message);
					break;
				case 3:
					handleMoveVerticesMessage((MoveVerticesMessage<V, E, M, Q>) message);
					break;
				case 4:
					handleUpdateRegisteredVerticesMessage((UpdateRegisteredVerticesMessage) message);
					break;

				default:
					logger.warn("Unknown incoming message id: " + message.getTypeCode());
					break;
			}
		}
		workerStats.HandleMessagesTime += (System.nanoTime() - startTime);
	}

	/**
	 * Handles an incoming control message
	 * @param message
	 * @return Returns true if message handling should be interrupted after this message
	 */
	public boolean handleControlMessage(ProtoEnvelopeMessage messageEnvelope) {
		ControlMessage message = messageEnvelope.message.getControlMessage();

		try {
			if (message != null) {
				switch (message.getType()) {
					case Master_Worker_Initialize:
						assignedPartitions = message.getAssignPartitions().getPartitionFilesList();
						masterStartTime = message.getAssignPartitions().getMasterStartTime();
						started = true;
						break;

					case Master_Query_Start:
						startQuery(deserializeQuery(message.getQueryValues()));
						break;

					case Master_Query_Next_Superstep: {
						lastWatchdogSignal = System.currentTimeMillis();
						handleMasterNextSuperstep(message);
					}
					break;

					case Master_Query_Finished: {
						Q query = deserializeQuery(message.getQueryValues());
						finishQuery(activeQueries.get(query.QueryId));
					}
					break;

					case Master_Start_Barrier: { // Start global barrier
						globalBarrierStartWaitSet.addAll(otherWorkerIds);
						globalBarrierStartWaitSet.removeAll(globalBarrierStartPrematureSet);
						globalBarrierFinishWaitSet.addAll(otherWorkerIds);
						globalBarrierFinishWaitSet.removeAll(globalBarrierFinishPrematureSet);
						globalBarrierSendVerts = message.getStartBarrier().getSendQueryVerticesList();
						List<ReceiveQueryVerticesMessage> recvVerts = message.getStartBarrier().getReceiveQueryVerticesList();
						globalBarrierRecvVerts = new HashSet<>(recvVerts.size());
						for (ReceiveQueryVerticesMessage rvMsg : recvVerts) {
							globalBarrierRecvVerts
							.add(new Pair<Integer, Integer>(rvMsg.getQueryId(), rvMsg.getReceiveFromMachine()));
						}
						globalBarrierRequested = true;
					}
					break;


					case Worker_Query_Superstep_Barrier: {
						Q query = deserializeQuery(message.getQueryValues());
						WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(query.QueryId);

						// We have to wait if the query is not already started
						if (activeQuery == null) {
							logger.debug("Postpone barrier message for not yet started query " + query.QueryId);
							List<ControlMessage> postponedForQuery = postponedBarrierMessages.get(query.QueryId);
							if (postponedForQuery == null) {
								postponedForQuery = new ArrayList<>();
								postponedBarrierMessages.put(query.QueryId, postponedForQuery);
							}
							postponedForQuery.add(message);
							return false;
						}

						handleQuerySuperstepBarrierMsg(message, activeQuery);
					}
					return true;

					case Worker_Barrier_Started: {
						//						System.out.println("Worker_Barrier_Started");
						int srcWorker = message.getSrcMachine();
						if (globalBarrierStartWaitSet.contains(srcWorker)) globalBarrierStartWaitSet.remove(srcWorker);
						else globalBarrierStartPrematureSet.add(srcWorker);
					}
					return true;
					case Worker_Barrier_Finished: {
						logger.debug(ownId + " Worker_Barrier_Finished");
						int srcWorker = message.getSrcMachine();
						if (globalBarrierFinishWaitSet.contains(srcWorker))
							globalBarrierFinishWaitSet.remove(srcWorker);
						else globalBarrierFinishPrematureSet.add(srcWorker);
					}
					return true;

					case Master_Shutdown: {
						logger.info("Received shutdown signal");
						stopRequested = true;
						stop();
					}
					break;

					default:
						logger.error("Unknown control message type: " + message);
						break;
				}
			}
		}
		catch (Throwable e) {
			logger.error("exception at incomingControlMessage", e);
		}
		return false;
	}


	// ++++++++++ Barrier Sync ++++++++++
	private void handleQuerySuperstepBarrierMsg(ControlMessage message, WorkerQuery<V, E, M, Q> activeQuery) {
		if (message.getSuperstepNo() == activeQuery.getBarrierSyncedSuperstep() + 1) {
			if (activeQuery.BarrierSyncWaitSet.remove(message.getSrcMachine())) {
				if (activeQuery.BarrierSyncWaitSet.isEmpty()) {
					activeQuery.onFinishedWorkerSuperstepBarrierSync(activeQuery.getBarrierSyncedSuperstep() + 1);
					// Wait for next barrier, apply all postponed premature barrier syncs
					activeQuery.BarrierSyncWaitSet.addAll(otherWorkerIds);

					// Notify master if compute and barrier sync finished
					if (activeQuery.isNextSuperstepLocallyReady()) {
						superstepLocalFinishNotifyMaster(activeQuery);
					}
				}
			}
			else {
				logger.error("Received worker barrier sync for worker not waiting for: " + message.getSrcMachine());
			}
		}
		else {
			// Completely wrong superstep
			logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: " + message.getSuperstepNo()
			+ " at " + activeQuery.Query.QueryId + ":" + activeQuery.getBarrierSyncedSuperstep());
		}
	}


	/**
	 * Handling for vertices which are moved
	 */
	private void handleVerticesMoving(List<AbstractVertex<V, E, M, Q>> verticesMoving, int queryId, int movedTo) {
		if (verticesMoving.isEmpty()) return;
		List<Integer> vertexMoveIds = verticesMoving.stream().map(v -> v.ID)
				.collect(Collectors.toCollection(ArrayList::new));

		// Broadcast vertex invalidate message
		for (int otherWorker : otherWorkerIds) {
			messaging.sendInvalidateRegisteredVerticesMessage(otherWorker, vertexMoveIds, movedTo, queryId);
		}

		// Remove vertices registry entries
		remoteVertexMachineRegistry.removeEntries(vertexMoveIds);
	}

	public void handleVertexMessage(VertexMessage<V, E, M, Q> message) {
		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);
		if (activeQuery == null) {
			logger.error("Cannot process vertex message, no active query for ID " + message.queryId + ". " + message);
			return;
		}
		int barrierSuperstepNo = activeQuery.getBarrierSyncedSuperstep();

		// Discover vertices if enabled. Only discover for broadcast messages as they are a sign that vertices are unknown.
		if (message.broadcastFlag && Configuration.VERTEX_MACHINE_DISCOVERY) {
			// Collect all vertices from this broadcast message on this machine
			final HashSet<Integer> srcVertices = new HashSet<>();
			for (final Pair<Integer, M> msg : message.vertexMessages) {
				if (localVertices.containsKey(msg.first)) {
					srcVertices.add(msg.first);
				}
			}
			// Also discover all source vertices from this incoming broadcast message, if enabled.
			if (Configuration.VERTEX_MACHINE_DISCOVERY_INCOMING) {
				for (final Integer srcVertId : srcVertices) {
					if (remoteVertexMachineRegistry.addEntry(srcVertId, message.srcMachine))
						activeQuery.QueryLocal.Stats.DiscoveredNewVertexMachines++;
				}
			}
			// Send get-to-know message for all own vertices in this broadcast message
			// Sender did not known these vertices, thats why a broadcast was sent.
			if (!srcVertices.isEmpty())
				messaging.sendGetToKnownMessage(message.srcMachine, srcVertices, message.queryId);
		}

		// Check if message arrived within correct barrier
		if (message.superstepNo == (barrierSuperstepNo + 1)) {
			for (final Pair<Integer, M> msg : message.vertexMessages) {
				final AbstractVertex<V, E, M, Q> msgVert = localVertices.get(msg.first);
				if (msgVert != null) {
					activeQuery.QueryLocal.Stats.MessagesReceivedCorrectVertex++;
					List<M> queryInMsgs = msgVert.queryMessagesNextSuperstep.get(message.queryId);
					if (queryInMsgs == null) {
						// Add queue if not already added
						queryInMsgs = new ArrayList<>();
						msgVert.queryMessagesNextSuperstep.put(message.queryId, queryInMsgs);
					}
					queryInMsgs.add(msg.second);
					// Activate vertex
					activeQuery.ActiveVerticesNext.put(msgVert.ID, msgVert);
				}
				else {
					if (!message.broadcastFlag) {
						logger.warn("Received non-broadcast vertex message for wrong vertex " + msg.first + " from "
								+ message.srcMachine + " query " + activeQuery.QueryId + ":" + barrierSuperstepNo
								+ " with no redirection");
					}
					activeQuery.QueryLocal.Stats.MessagesReceivedWrongVertex++;
				}
			}
		}
		else {
			logger.error("VertexMessage from wrong barrier superstepNo: " + message.superstepNo + " should be "
					+ (barrierSuperstepNo + 1) + " from " + message.srcMachine);
		}
		message.free(false);
	}


	public void handleGetToKnowMessage(GetToKnowMessage message) {
		for (final Integer srcVertex : message.vertices) {
			if (remoteVertexMachineRegistry.addEntry(srcVertex, message.srcMachine))
				activeQueries.get(message.queryId).QueryLocal.Stats.DiscoveredNewVertexMachines++;
		}
	}


	public void handleMoveVerticesMessage(MoveVerticesMessage<V, E, M, Q> message) {
		long startTime = System.nanoTime();

		if (message.lastSegment) {
			// Remove from globalBarrierRecvVerts if received all vertices
			globalBarrierRecvVerts.remove(new Pair<>(message.queryId, message.srcMachine));
		}

		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);
		if (activeQuery == null)
			return;

		for (AbstractVertex<V, E, M, Q> movedVert : message.vertices) {
			activeQuery.ActiveVerticesThis.put(movedVert.ID, movedVert); // TODO Handle if vertices of intersecting queries
			activeQuery.ActiveVerticesNext.put(movedVert.ID, movedVert); // TODO ActiveVerticesNext?
			localVertices.put(movedVert.ID, movedVert);
		}

		//		if (message.lastSegment) {
		//		}

		long moveTime = (System.nanoTime() - startTime);
		activeQuery.QueryLocal.Stats.MoveRecvVertices += message.vertices.size();
		activeQuery.QueryLocal.Stats.MoveRecvVerticesTime += moveTime;
	}


	public void handleUpdateRegisteredVerticesMessage(UpdateRegisteredVerticesMessage message) {
		WorkerQuery<V, E, M, Q> query = activeQueries.get(message.queryId);
		assert (query != null);
		int updatedEntries;
		if (message.movedTo != ownId)
			updatedEntries = remoteVertexMachineRegistry.updateEntries(message.vertices, message.movedTo);
		else
			updatedEntries = remoteVertexMachineRegistry.removeEntries(message.vertices);
		query.QueryLocal.Stats.UpdateVertexRegisters += updatedEntries;
	}


	/**
	 * Called after Master_Query_Next_Superstep message.
	 * Prepare next superstep and start if already possible.
	 */
	private void handleMasterNextSuperstep(ControlMessage message) {
		Q msgQuery = deserializeQuery(message.getQueryValues());
		WorkerQuery<V, E, M, Q> query = activeQueries.get(msgQuery.QueryId);

		// Checks
		if (query == null) {
			logger.error("Received Master_Next_Superstep for unknown query: " + message);
			return;
		}
		if (message.getSuperstepNo() != query.getMasterStartedSuperstep() + 1) {
			logger.error("Wrong superstep number to start next: " + query.QueryId + ":" + message.getSuperstepNo()
			+ " should be " + (query.getMasterStartedSuperstep() + 1) + ", "
			+ query.getSuperstepNosLog());
			return;
		}

		// New query stats for next superstep
		query.Query = msgQuery;
		query.QueryLocal = globalValueFactory.createClone(msgQuery);
		workerStats.addQueryStatsstepStats(query.QueryLocal.Stats);
		query.QueryLocal.Stats = new QueryStats();

		// Finish superstep, start next
		query.onMasterNextSuperstep(message.getSuperstepNo());
		logger.trace(
				"Worker finished superstep, ready for next " + query.Query.QueryId + ":"
						+ query.getMasterStartedSuperstep()
						+ ". Active: " + query.QueryLocal.getActiveVertices());
	}

	/**
	 * Finishes the superstep locally and notifies master.
	 * Called after compute and barrier finished
	 */
	private void superstepLocalFinishNotifyMaster(WorkerQuery<V, E, M, Q> query) {
		// Finish superstep active vertices, prepare for next superstep
		long startTime = System.nanoTime();
		ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> swap = query.ActiveVerticesThis;
		query.ActiveVerticesThis = query.ActiveVerticesNext;
		query.ActiveVerticesNext = swap;
		query.ActiveVerticesNext.clear();

		// Prepare active vertices, now we have all vertex messages and compute is finished
		Integer queryId = query.Query.QueryId;
		for (AbstractVertex<V, E, M, Q> vert : query.ActiveVerticesThis.values()) {
			vert.prepareForNextSuperstep(queryId);
		}

		// Reset active vertices
		query.QueryLocal.setActiveVertices(query.ActiveVerticesThis.size());

		// Finish barrier
		query.QueryLocal.Stats.StepFinishTime += System.nanoTime() - startTime;

		// Notify master that superstep finished
		Map<Integer, Integer> queryIntersects = calculateQueryIntersects(query); // TODO Optimize, dont calculate always?
		sendMasterSuperstepFinished(query, queryIntersects);
		query.onLocalFinishSuperstep(query.getMasterStartedSuperstep());

		logger.trace("Worker finished local superstep " + query.Query.QueryId + ":"
				+ query.getMasterStartedSuperstep() + ". Active: " + query.ActiveVerticesThis.size());
	}



	// #################### Message sending #################### //
	public void flushVertexMessages(WorkerQuery<V, E, M, Q> query) {
		sendBroadcastVertexMessageBucket(query, query.getMasterStartedSuperstep());
		for (final int otherWorkerId : otherWorkerIds) {
			final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(otherWorkerId);
			if (!msgBucket.messages.isEmpty())
				sendUnicastVertexMessageBucket(msgBucket, otherWorkerId, query, query.getMasterStartedSuperstep());
			messaging.flushAsyncChannel(otherWorkerId);
		}
	}


	private void sendWorkersSuperstepFinished(WorkerQuery<V, E, M, Q> workerQuery) {
		messaging.sendControlMessageMulticast(otherWorkerIds,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepBarrier(workerQuery.getMasterStartedSuperstep(),
						ownId,
						workerQuery.Query),
				true);
	}

	private void sendMasterInitialized() {
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Initialized(ownId, localVertices.size()),
				true);
	}

	private void sendMasterSuperstepFinished(WorkerQuery<V, E, M, Q> workerQuery, Map<Integer, Integer> queryIntersects) {
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(
						workerQuery.getMasterStartedSuperstep(),
						ownId,
						workerQuery.QueryLocal, queryIntersects, workerStatsSamplesToSend),
				true);
		workerStatsSamplesToSend.clear();
	}

	private void sendMasterQueryFinishedMessage(WorkerQuery<V, E, M, Q> workerQuery) {
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QueryFinished(workerQuery.getMasterStartedSuperstep(), ownId,
						workerQuery.QueryLocal),
				true);
	}


	/**
	 * Sends a vertex message. If local vertex, direct loopback. It remote vertex try to lookup machine. If machine not known broadcast message.
	 */
	@Override
	public void sendVertexMessage(int dstVertex, M messageContent, WorkerQuery<V, E, M, Q> query) {
		final AbstractVertex<V, E, M, Q> msgVert = localVertices.get(dstVertex);
		if (msgVert != null) {
			// Local message
			query.QueryLocal.Stats.MessagesTransmittedLocal++;
			List<M> queryInMsgs = msgVert.queryMessagesNextSuperstep.get(query.QueryId);
			if (queryInMsgs == null) {
				// Add queue if not already added
				queryInMsgs = new ArrayList<>();
				msgVert.queryMessagesNextSuperstep.put(query.QueryId, queryInMsgs);
			}
			queryInMsgs.add(messageContent);
			// Activate vertex
			query.ActiveVerticesNext.put(msgVert.ID, msgVert);
		}
		else {
			// Remote message
			final Integer remoteMachine = remoteVertexMachineRegistry.lookupEntry(dstVertex);
			if (remoteMachine != null) {
				// Unicast remote message
				sendVertexMessageToMachine(remoteMachine, dstVertex, query, query.getMasterStartedSuperstep(),
						messageContent);
			}
			else {
				// Broadcast remote message
				query.QueryLocal.Stats.MessagesSentBroadcast += otherWorkerIds.size();
				vertexMessageBroadcastBucket.addMessage(dstVertex, messageContent);
				if (vertexMessageBroadcastBucket.messages.size() > Configuration.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
					sendBroadcastVertexMessageBucket(query, query.getMasterStartedSuperstep());
				}
			}
		}
	}

	/**
	 * Sends a vertex message directly to a remote machine, no lookup.
	 */
	public void sendVertexMessageToMachine(int dstMachine, int dstVertex, WorkerQuery<V, E, M, Q> query, int superstepNo,
			M messageContent) {
		final VertexMessageBucket<M> msgBucket = vertexMessageMachineBuckets.get(dstMachine);
		if (msgBucket == null)
			logger.error("No vertex message bucket for machine " + dstMachine);
		msgBucket.addMessage(dstVertex, messageContent);
		query.QueryLocal.Stats.MessagesSentUnicast++;
		if (msgBucket.messages.size() > Configuration.VERTEX_MESSAGE_BUCKET_MAX_MESSAGES - 1) {
			sendUnicastVertexMessageBucket(msgBucket, dstMachine, query, superstepNo);
		}
	}

	private void sendUnicastVertexMessageBucket(VertexMessageBucket<M> msgBucket, int dstMachine, WorkerQuery<V, E, M, Q> query,
			int superstepNo) {
		final List<Pair<Integer, M>> msgList = packVertexMessage(msgBucket);
		messaging.sendVertexMessageUnicast(dstMachine, superstepNo, ownId, query.QueryId, msgList);
		query.QueryLocal.Stats.MessageBucketsSentUnicast++;
	}

	private void sendBroadcastVertexMessageBucket(WorkerQuery<V, E, M, Q> query, int superstepNo) {
		if (vertexMessageBroadcastBucket.messages.isEmpty()) return;
		final List<Pair<Integer, M>> msgList = packVertexMessage(vertexMessageBroadcastBucket);
		messaging.sendVertexMessageBroadcast(otherWorkerIds, superstepNo, ownId, query.QueryId, msgList);
		query.QueryLocal.Stats.MessageBucketsSentBroadcast++;
	}

	private List<Pair<Integer, M>> packVertexMessage(VertexMessageBucket<M> msgBucket) {
		final List<Pair<Integer, M>> msgList = new ArrayList<>(msgBucket.messages); // Could Pool instance
		msgBucket.messages.clear();
		return msgList;
	}



	// #################### Vertex Move #################### //
	/**
	 * Sends all active vertices of a query if they are only active at this query.
	 * Used for incremental vertex migration.
	 */
	private void sendQueryVerticesToMove(int queryId, int sendToWorker, int maxVertices) {
		long startTime = System.nanoTime();
		int verticesSent = 0;
		int verticesMessagesSent = 0;
		WorkerQuery<V, E, M, Q> query = activeQueries.get(queryId);
		List<AbstractVertex<V, E, M, Q>> verticesToMove = new ArrayList<>();

		//		if (movedQueryVertices.containsKey(queryId))
		//			logger.error("movedVerticesRedirections not cleaned up after last superstep");

		int moved = 0;
		int notMoved = 0;

		if (query != null) {
			//			if (getQueryIntersectionCount(queryId) == 0) { // Only move vertices if no intersection
			Map<Integer, WorkerQuery<V, E, M, Q>> otherQueries = new HashMap<>(activeQueries);
			otherQueries.remove(queryId);

			for (AbstractVertex<V, E, M, Q> vertex : query.ActiveVerticesThis.values()) {
				// TODO Currently only sending nonintersecting vertices. Handle ActiveVerticesThis
				// Check for intersection, dont move
				boolean dontMove = false;
				for (WorkerQuery<V, E, M, Q> otherQuery : otherQueries.values()) {
					if (otherQuery.ActiveVerticesThis.containsKey(vertex.ID)) {
						dontMove = true;
					}
				}
				if (dontMove) {
					notMoved++;
					continue;
				}
				moved++;

				localVertices.remove(vertex.ID);
				query.ActiveVerticesThis.remove(vertex.ID);
				query.ActiveVerticesNext.remove(vertex.ID); // TODO Why ActiveVerticesNext?
				verticesToMove.add(vertex);
				// Send vertices now if bucket full
				if (verticesToMove.size() >= Configuration.VERTEX_MOVE_BUCKET_MAX_VERTICES) {
					handleVerticesMoving(verticesToMove, query.QueryId, sendToWorker);
					messaging.sendMoveVerticesMessage(sendToWorker, verticesToMove, queryId, false);
					verticesSent += verticesToMove.size();
					verticesToMove = new ArrayList<>();
				}
				if (Configuration.DETAILED_STATS) {
					verticesMessagesSent += vertex.getBufferedMessageCount();
				}
			}
			logger.info(ownId + " Sent " + moved + " and skipped " + notMoved + " to " + sendToWorker + " for query " + queryId); // TODO logger.debug
			//			}
			//			else {
			//				logger.info(queryId + " vertices not moved because has intersection now: "
			//						+ currentQueryIntersects.get(queryId));
			//			}
		}
		else {
			logger.info("Vertices not moved because query inactive: " + queryId);
		}

		// Send remaining vertices (or empty message if none)
		handleVerticesMoving(verticesToMove, queryId, sendToWorker);
		messaging.sendMoveVerticesMessage(sendToWorker, verticesToMove, queryId, true);
		verticesSent += verticesToMove.size();

		if (query != null) {
			query.QueryLocal.Stats.MoveSendVertices += verticesSent;
			query.QueryLocal.Stats.MoveSendVerticesTime += (System.nanoTime() - startTime);
			query.QueryLocal.Stats.MoveSendVerticesMessages += verticesMessagesSent;
		}
	}



	// #################### Others #################### //
	private Map<Integer, Integer> calculateQueryIntersects(WorkerQuery<V, E, M, Q> activeQuery) {
		Map<Integer, Integer> queryIntersects = new HashMap<>();
		long startTime2 = System.nanoTime();
		for (WorkerQuery<V, E, M, Q> otherQuery : activeQueries.values()) {
			if (otherQuery.QueryId == activeQuery.QueryId) continue;
			int intersects;
			intersects = MiscUtil.getIntersectCount(activeQuery.ActiveVerticesThis.keySet(),
					otherQuery.ActiveVerticesThis.keySet());
			queryIntersects.put(otherQuery.QueryId, intersects);
		}
		activeQuery.QueryLocal.Stats.IntersectCalcTime += System.nanoTime() - startTime2;

		// Update currentQueryIntersects
		currentQueryIntersects.put(activeQuery.QueryId, queryIntersects);
		for (Entry<Integer, Integer> intersection : queryIntersects.entrySet()) {
			Map<Integer, Integer> otherIntersects = currentQueryIntersects.get(intersection.getKey());
			if (otherIntersects == null) {
				otherIntersects = new HashMap<>();
				currentQueryIntersects.put(intersection.getKey(), otherIntersects);
			}
			otherIntersects.put(activeQuery.QueryId, intersection.getValue());
		}
		return queryIntersects;
	}

	private void sampleWorkerStats() {
		long activeVertices = 0;
		for (WorkerQuery<V, E, M, Q> query : activeQueries.values())
			activeVertices += query.ActiveVerticesThis.size();

		// Output a sample of vertices on this machine
		int allVertSampleRate = Configuration.getPropertyIntDefault("WorkerStatsAllVerticesSampleRate", 0);
		if (allVertSampleRate > 0) {
			if (allVertexStatsFile == null) {
				try {
					allVertexStatsFile = new PrintWriter(new FileWriter(outputDir + File.separator + "stats"
							+ File.separator + "worker" + ownId + "_allVertexStats.txt"));
				}
				catch (IOException e) {
					logger.error("Failed to create vertexStats file", e);
				}
			}

			if (allVertexStatsFile != null) {
				Random random = new Random(0);
				StringBuilder sb = new StringBuilder();
				for (int v : localVertices.keySet()) {
					if (random.nextInt(allVertSampleRate) != 0) continue;
					sb.append(v);
					sb.append(';');
				}
				allVertexStatsFile.println(sb.toString());
			}
		}

		int actVertSampleRate = Configuration.getPropertyIntDefault("WorkerStatsActiveVerticesSampleRate", 0);
		if (actVertSampleRate > 0) {
			if (actVertexStatsFile == null) {
				try {
					actVertexStatsFile = new PrintWriter(new FileWriter(outputDir + File.separator + "stats"
							+ File.separator + "worker" + ownId + "_actVertexStats.txt"));
				}
				catch (IOException e) {
					logger.error("Failed to create vertexStats file", e);
				}
			}

			if (actVertexStatsFile != null) {
				Random random = new Random(0);
				StringBuilder sb = new StringBuilder();
				Set<Integer> activeVerts = new HashSet<>();
				for (WorkerQuery<V, E, M, Q> query : activeQueries.values()) {
					for (AbstractVertex<V, E, M, Q> v : query.ActiveVerticesThis.values()) {
						activeVerts.add(v.ID);
					}
				}

				for (int v : activeVerts) {
					if (random.nextInt(actVertSampleRate) != 0) continue;
					sb.append(v);
					sb.append(';');
				}
				actVertexStatsFile.println(sb.toString());
			}
		}

		workerStats.ActiveVertices = activeVertices;
		workerStats.WorkerVertices = localVertices.size();
		workerStatsSamplesToSend.add(workerStats.getSample(System.currentTimeMillis() - masterStartTime));
		workerStats = new WorkerStats();
	}

	@Override
	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}

	private Q deserializeQuery(ByteString bytes) {
		return globalValueFactory.createFromBytes(ByteBuffer.wrap(bytes.toByteArray()));
	}
}
