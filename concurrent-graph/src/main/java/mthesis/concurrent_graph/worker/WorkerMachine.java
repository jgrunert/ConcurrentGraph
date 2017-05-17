package mthesis.concurrent_graph.worker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.BaseQuery.SuperstepInstructions;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.GetToKnowMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryChunkMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryChunkMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.Messages.WorkerQueryExecutionMode;
import mthesis.concurrent_graph.communication.MoveVerticesMessage;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.communication.UpdateRegisteredVerticesMessage;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.communication.VertexMessageBucket;
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
	// List of finished queries, not older than QUERY_CUT_TIME_WINDOW
	private final List<WorkerQuery<V, E, M, Q>> queriesHistoryList = new ArrayList<>();
	private final Map<Integer, WorkerQuery<V, E, M, Q>> queriesHistoryMap = new HashMap<>();
	// Counter of supersteps/localSupersteps per query. Resets when query vertices moved.
	private final Map<Integer, Pair<Integer, Integer>> queriesLocalSupersteps = new HashMap<>();
	// Barrier messages that arrived before a became was active
	private final Map<Integer, List<ControlMessage>> postponedBarrierMessages = new HashMap<>();

	private final VertexMachineRegistry remoteVertexMachineRegistry = new VertexMachineRegistry();

	// Buckets to collect messages to send
	private final VertexMessageBucket<M> vertexMessageBroadcastBucket = new VertexMessageBucket<>();
	private final Map<Integer, VertexMessageBucket<M>> vertexMessageMachineBuckets = new HashMap<>();

	private final BlockingQueue<ChannelMessage> receivedMessages = new LinkedBlockingQueue<>();

	// Global barrier coordination/control
	private boolean globalBarrierRequested = false;
	private volatile boolean globalBarrierVertexMoveActive = false;
	private Map<Integer, Integer> globalBarrierQuerySupersteps;
	private final Set<Integer> globalBarrierStartWaitSet = new HashSet<>();
	private final Set<Integer> globalBarrierStartPrematureSet = new HashSet<>();
	private final Set<Integer> globalBarrierReceivingFinishWaitSet = new HashSet<>();
	private final Set<Integer> globalBarrierReceivingFinishPrematureSet = new HashSet<>();
	private final Set<Integer> globalBarrierFinishWaitSet = new HashSet<>();
	private final Set<Integer> globalBarrierFinishPrematureSet = new HashSet<>();
	// Global barrier commands to perform while barrier
	private List<SendQueryChunkMessage> globalBarrierSendVerts;
	private Set<Pair<Set<Integer>, Integer>> globalBarrierRecvVerts;
	private List<MoveVerticesMessage<V, E, M, Q>> queuedMoveMessages = new ArrayList<>();

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

	private long lastSendMasterQueryIntersects;

	/** Map of all queries active vertices in past barrier periods. Disabled if no vertex barrier move enabled */
	//private List<Int2ObjectMap<IntSet>> queryActiveVerticesSlidingWindow = new ArrayList<>();

	//Int2ObjectMap<IntSet> queryActiveVerticesSinceBarrier = new Int2ObjectOpenHashMap<>();

	// Most recent calculated query intersections
	//	private Map<Integer, Map<Integer, Integer>> currentQueryIntersects = new HashMap<>();

	public static long lastUpdateTime = System.currentTimeMillis();



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

		lastSendMasterQueryIntersects = System.currentTimeMillis();
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
			throw new RuntimeException("Query with this ID already active: " + query.QueryId);
		WorkerQuery<V, E, M, Q> activeQuery = new WorkerQuery<>(query, globalValueFactory, localVertices.keySet(),
				System.currentTimeMillis());
		activeQuery.BarrierSyncWaitSet.addAll(otherWorkerIds);

		activeQueries.put(query.QueryId, activeQuery);

		// Handle postponed barrier messages if there are any
		List<ControlMessage> postponedBarrierMsgs = postponedBarrierMessages.get(query.QueryId);
		if (postponedBarrierMsgs != null) {
			for (ControlMessage message : postponedBarrierMsgs) {
				handleQuerySuperstepBarrierMsg(message, activeQuery);
			}
		}

		logger.debug("Worker started query {}", query.QueryId);
	}

	private void finishQuery(WorkerQuery<V, E, M, Q> activeQuery) {
		new VertexTextOutputWriter<V, E, M, Q>().writeOutput(
				outputDir + File.separator + activeQuery.Query.QueryId + File.separator + ownId + ".txt", localVertices.values(),
				activeQuery.Query.QueryId);
		sendMasterQueryFinishedMessage(activeQuery);
		for (final AbstractVertex<V, E, M, Q> vertex : localVertices.values()) {
			vertex.finishQuery(activeQuery.Query.QueryId);
		}
		logger.debug("Worker finished query {}", activeQuery.Query.QueryId);
		activeQueries.remove(activeQuery.Query.QueryId);
		if (Configuration.VERTEX_BARRIER_MOVE_ENABLED) {
			queriesHistoryList.add(activeQuery);
			queriesHistoryMap.put(activeQuery.QueryId, activeQuery);
		}
	}



	// #################### Running #################### //
	@Override
	public void run() {
		logger.info("Starting run worker node " + ownId);

		boolean interrupted = false;
		try {
			while (!started) {
				handleReceivedMessagesWait();
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
					handleReceivedMessagesWait();
				}
				workerStats.IdleTime += (System.nanoTime() - startTime);


				// ++++++++++ Wait for queries ready to compute ++++++++++
				startTime = System.nanoTime();
				activeQueriesThisStep.clear();
				while (!stopRequested && activeQueriesThisStep.isEmpty()
						&& !(globalBarrierRequested && checkQueriesReadyForBarrier())) {
					for (WorkerQuery<V, E, M, Q> activeQuery : activeQueries.values()) {
						if (activeQuery.getMasterStartedSuperstep() == activeQuery.getLastFinishedComputeSuperstep()
								+ 1)
							activeQueriesThisStep.add(activeQuery);
					}
					if (!activeQueriesThisStep.isEmpty() || (globalBarrierRequested && checkQueriesReadyForBarrier()))
						break;

					handleReceivedMessagesWait();

					if ((System.nanoTime() - startTime) > 10000000000L) {// Warn after 10s
						//if ((System.nanoTime() - startTime) > 1000000000L) {// Warn after 1s
						logger.warn("Waiting long time for active queries");
						Thread.sleep(2000);
						//Thread.sleep(200);
					}
				}
				//logger.info("+++ " + activeQueriesThisStep);
				workerStats.QueryWaitTime += (System.nanoTime() - startTime);


				// ++++++++++ Global barrier if requested and no more outstanding queries ++++++++++
				if (globalBarrierRequested && activeQueriesThisStep.isEmpty() && checkQueriesReadyForBarrier()) {

					// --- Start barrier, notify other workers  ---
					logger.debug("Barrier started, waiting for other workers to start");
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Started(ownId),
							true);


					// --- Wait for other workers barriers ---
					startTime = System.nanoTime();
					while (!globalBarrierStartWaitSet.isEmpty()) {
						handleReceivedMessagesWait();
					}
					long barrierStartWaitTime = System.nanoTime() - startTime;


					// --- Checks  ---
					for (WorkerQuery<V, E, M, Q> query : activeQueries.values()) {
						//querySSs += query.QueryId + ":" + query.getMasterStartedSuperstep() + " ";
						if (globalBarrierQuerySupersteps.containsKey(query.QueryId)
								&& !globalBarrierQuerySupersteps.get(query.QueryId).equals(query.getLastFinishedComputeSuperstep())) {
							logger.warn("Query " + query.QueryId + " is not ready for global barrier, wrong superstep: "
									+ query.getLastFinishedComputeSuperstep() + " should be "
									+ globalBarrierQuerySupersteps.get(query.QueryId));
						}
						if (query.getMasterStartedSuperstep() != query.getLastFinishedComputeSuperstep()
								&& query.getMasterStartedSuperstep() >= 0) {
							logger.warn("Query " + query.QueryId + " is not ready for global barrier, barrier superstep: "
									+ query.getMasterStartedSuperstep() + " " + query.getLastFinishedComputeSuperstep());
						}
					}


					// --- Send and receive vertices ---
					startTime = System.nanoTime();
					globalBarrierVertexMoveActive = true;
					// Send vertices
					long timmmm = System.currentTimeMillis();
					System.out.println(ownId + " start");
					for (SendQueryChunkMessage sendVert : globalBarrierSendVerts) {
						sendQueryVerticesToMove(new HashSet<>(sendVert.getChunkQueriesList()), sendVert.getMoveToMachine(),
								sendVert.getMaxMoveCount());
					}
					System.out.println(ownId + " stap " + (System.currentTimeMillis() - timmmm));

					// Wait until we received all messages
					while (!globalBarrierRecvVerts.isEmpty()) {
						handleReceivedMessagesWait();
					}

					// Send barrier message and flush to notify others that received everything
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Receiving_Finished(ownId), true);

					// Wait until everyone sent and received all messages - then we can continue without causing problems
					while (!globalBarrierReceivingFinishWaitSet.isEmpty()) {
						handleReceivedMessagesWait();
					}
					globalBarrierVertexMoveActive = false;


					// --- Process queued move messages ---
					processQueuedMoveVerticesMessages();
					workerStats.BarrierVertexMoveTime += (System.nanoTime() - startTime);
					startTime = System.nanoTime();


					// --- Finish barrier, notify other workers and master ---
					logger.debug("Global barrier tasks done, waiting for other workers to finish");
					messaging.sendControlMessageMulticast(otherWorkerIds,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Finished(ownId), true);
					messaging.sendControlMessageUnicast(masterId,
							ControlMessageBuildUtil.Build_Worker_Worker_Barrier_Finished(ownId), true);
					while (!globalBarrierFinishWaitSet.isEmpty()) {
						handleReceivedMessagesWait();
					}
					long barrierFinishWaitTime = System.nanoTime() - startTime;

					logger.debug("Global barrier finished on worker {}", ownId);
					workerStats.BarrierStartWaitTime += barrierStartWaitTime;
					workerStats.BarrierFinishWaitTime += barrierFinishWaitTime;
					globalBarrierRequested = false;

					// Synchronize query intersects sending
					lastSendMasterQueryIntersects = System.currentTimeMillis();

					if (!globalBarrierStartPrematureSet.isEmpty()) {
						logger.warn("globalBarrierStartPrematureSet not empty");
						globalBarrierStartPrematureSet.clear();
					}
					if (!globalBarrierReceivingFinishPrematureSet.isEmpty()) {
						logger.warn("globalBarrierReceivingFinishPrematureSet not empty");
						globalBarrierReceivingFinishPrematureSet.clear();
					}
					if (!globalBarrierFinishPrematureSet.isEmpty()) {
						logger.warn("globalBarrierFinishPrematureSet not empty");
						globalBarrierFinishPrematureSet.clear();
					}
				}


				// ++++++++++ Compute active and ready queries ++++++++++
				for (WorkerQuery<V, E, M, Q> activeQuery : activeQueriesThisStep) {
					lastUpdateTime = System.currentTimeMillis();

					int queryId = activeQuery.Query.QueryId;
					int superstepNo = activeQuery.getMasterStartedSuperstep();
					SuperstepInstructions stepInstructions = activeQuery.QueryLocal.onWorkerSuperstepStart(superstepNo);

					// Next superstep. Compute and Messaging (done by vertices)
					logger.trace("Worker start superstep compute {}:{}", new Object[] { queryId, superstepNo });

					// First frame: Call all vertices, second frame only active vertices TODO more flexible, call single vertex
					startTime = System.nanoTime();

					do {
						boolean isLocalSuperstep = activeQuery.localExecution;

						if (superstepNo >= 0) {
							switch (stepInstructions.type) {
								case StartActive:
									for (AbstractVertex<V, E, M, Q> vertex : activeQuery.ActiveVerticesThis.values()) {
										vertex.superstep(superstepNo, activeQuery, false);
									}
									break;
								case StartSpecific:
									for (int vertexId : stepInstructions.specificVerticesIds) {
										AbstractVertex<V, E, M, Q> vertex = localVertices.get(vertexId);
										if (vertex != null)
											vertex.superstep(superstepNo, activeQuery, true);
									}
									break;
								case StartAll:
									for (final AbstractVertex<V, E, M, Q> vertex : localVertices.values()) {
										vertex.superstep(superstepNo, activeQuery, true);
									}
									break;
								default:
									logger.error("Unsupported stepInstructions type: " + stepInstructions.type);
									break;
							}
						}

						// Supersteps/LocalSupersteps counters
						Pair<Integer, Integer> querySuperstepsStat = queriesLocalSupersteps.get(activeQuery.QueryId);
						if (querySuperstepsStat == null) querySuperstepsStat = new Pair<Integer, Integer>(0, 0);
						if (isLocalSuperstep) {
							activeQuery.QueryLocal.Stats.LocalSuperstepsComputed++;
							querySuperstepsStat.second++;
						}
						activeQuery.QueryLocal.Stats.SuperstepsComputed++;
						querySuperstepsStat.first++;
						queriesLocalSupersteps.put(activeQuery.QueryId, querySuperstepsStat);

						// Check if local execution possible
						if (activeQuery.localExecution) {
							// TODO Continue running in local mode
							if (activeQuery.ActiveVerticesNext.isEmpty()) {
								// Query local and no more vertices active - finished
								logger.debug("No more vertices active while local query execution {}:{}",
										new Object[] { queryId, superstepNo });
								if (superstepNo != activeQuery.getBarrierSyncedSuperstep())
									activeQuery.onFinishedWorkerSuperstepBarrierSync(superstepNo);
								finishNonlocalSuperstepCompute(activeQuery);
								activeQuery.localExecution = false;
								break;
							}
							else {
								// Continue query local execution
								logger.trace("Local query execution continues {}:{}", new Object[] { queryId, superstepNo });
								activeQuery.onFinishedLocalmodeSuperstepCompute(superstepNo);
								activeQuery.onLocalFinishSuperstep(superstepNo);
								activeQuery.onMasterNextSuperstep(superstepNo + 1,
										WorkerQueryExecutionMode.LocalOnThis);
								finishQuerySuperstep(activeQuery);
								superstepNo++;
							}
						}
						else {
							// Nonlocal query execution or local execution stopped
							if (isLocalSuperstep) {
								logger.debug(
										"Local query execution ended {}:{}", new Object[] { queryId, superstepNo });
								activeQuery.QueryLocal.Stats.LocalmodeStops++;
								// Skip barrier if query was local for more than one superstep
								if (superstepNo != activeQuery.getBarrierSyncedSuperstep())
									activeQuery.onFinishedWorkerSuperstepBarrierSync(superstepNo);
							}
							finishNonlocalSuperstepCompute(activeQuery);
							break;
						}
					} while ((System.nanoTime() - startTime) <= Configuration.WORKER_LOCAL_EXECUTE_TIME_LIMIT);

					long computeTime = System.nanoTime() - startTime;
					activeQuery.QueryLocal.Stats.ComputeTime += computeTime;
				}


				// ++++++++++ Worker stats ++++++++++
				if ((System.currentTimeMillis() - workerStatsLastSample) >= Configuration.WORKER_STATS_SAMPLING_INTERVAL) {
					sampleWorkerStats();
					workerStatsLastSample = System.currentTimeMillis();
				}

				if (Configuration.VERTEX_BARRIER_MOVE_ENABLED &&
						(System.currentTimeMillis() - lastSendMasterQueryIntersects) >= Configuration.WORKER_QUERY_INTERSECT_INTERVAL) {
					Map<IntSet, Integer> intersectsSampled = calculateQueryIntersectChunksSampled(10);// TODO Config
					MessageEnvelope msg = ControlMessageBuildUtil.Build_Worker_QueryVertexChunks(ownId, intersectsSampled,
							activeQueries.keySet(),
							queriesLocalSupersteps);
					messaging.sendControlMessageUnicast(masterId, msg, true);
					lastSendMasterQueryIntersects = System.currentTimeMillis();
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

	// Checks if all queries are ready for a global barrier
	private boolean checkQueriesReadyForBarrier() {
		if (!globalBarrierRequested) return false;
		for (WorkerQuery<V, E, M, Q> query : activeQueries.values()) {
			if (globalBarrierQuerySupersteps.containsKey(query.QueryId) && !globalBarrierQuerySupersteps
					.get(query.QueryId).equals(query.getLastFinishedComputeSuperstep())) {
				return false;
			}
		}
		return true;
	}

	private void finishNonlocalSuperstepCompute(WorkerQuery<V, E, M, Q> activeQuery) {
		int superstepNo = activeQuery.getMasterStartedSuperstep();

		// Send barrier sync with other workers
		if (!otherWorkerIds.isEmpty()) {
			flushVertexMessages(activeQuery);
			sendWorkersSuperstepFinished(activeQuery);
		}

		activeQuery.onFinishedSuperstepCompute(superstepNo);
		logger.trace("Worker finished nonlocal compute {}:{}", new Object[] { activeQuery.Query.QueryId, superstepNo });

		// Notify master if compute and barrier sync finished
		if (activeQuery.isNextSuperstepLocallyReady()) {
			superstepLocalFinishNotifyMaster(activeQuery);
		}
	}

	private void skipSuperstepCompute(WorkerQuery<V, E, M, Q> activeQuery) {
		int superstepNo = activeQuery.getMasterStartedSuperstep();

		activeQuery.onFinishedSuperstepCompute(superstepNo);
		logger.trace("Worker skipped compute {}:{}", new Object[] { activeQuery.Query.QueryId, superstepNo });

		// Notify master if compute and barrier sync finished
		if (activeQuery.isNextSuperstepLocallyReady()) {
			superstepLocalFinishNotifyMaster(activeQuery);
		}
	}



	// #################### Handle incoming messages #################### //
	@Override
	public void onIncomingMessage(ChannelMessage message) {
		//		if (message.getTypeCode() == 1) {
		//			ProtoEnvelopeMessage msgEnvelope = ((ProtoEnvelopeMessage) message);
		//			if (msgEnvelope.message.hasControlMessage()) {
		//				ControlMessage controlMsg = msgEnvelope.message.getControlMessage();
		//				if (controlMsg.getType() == ControlMessageType.Master_Query_Next_Superstep) {
		//					Q msgQuery = deserializeQuery(controlMsg.getQueryValues());
		//					logger.info("-- " + msgQuery.QueryId + ":" + controlMsg.getSuperstepNo());
		//				}
		//			}
		//		}
		receivedMessages.add(message);
	}


	/**
	 * Handles all messages in receive queue.
	 * Waits until at least one message has been handled.
	 */
	private void handleReceivedMessagesWait() throws InterruptedException {
		handleWaitNextReceivedMessage();
		handleReceivedMessagesNoWait();
	}

	/**
	 * Handles all messages in receive queue.
	 * Returns when no more messages are there
	 */
	private void handleReceivedMessagesNoWait() {
		long startTime = System.nanoTime();
		ChannelMessage message;
		while ((message = receivedMessages.poll()) != null) {
			handleReceivedMessage(message);
		}
		workerStats.HandleMessagesTime += (System.nanoTime() - startTime);
	}

	/**
	 * Waits for the next received message and handles it.
	 */
	private void handleWaitNextReceivedMessage() throws InterruptedException {
		ChannelMessage message = receivedMessages.take();
		handleReceivedMessage(message);
	}

	@SuppressWarnings("unchecked")
	private void handleReceivedMessage(ChannelMessage message) {
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
				handleIncomingMoveVerticesMessage((MoveVerticesMessage<V, E, M, Q>) message);
				break;
			case 4:
				handleUpdateRegisteredVerticesMessage((UpdateRegisteredVerticesMessage) message);
				break;

			default:
				logger.warn("Unknown incoming message id: " + message.getTypeCode());
				break;
		}
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
						StartBarrierMessage startBarrierMsg = message.getStartBarrier();
						globalBarrierStartWaitSet.addAll(otherWorkerIds);
						globalBarrierStartWaitSet.removeAll(globalBarrierStartPrematureSet);
						globalBarrierStartPrematureSet.clear();
						globalBarrierReceivingFinishWaitSet.addAll(otherWorkerIds);
						globalBarrierReceivingFinishWaitSet.removeAll(globalBarrierReceivingFinishPrematureSet);
						globalBarrierReceivingFinishPrematureSet.clear();
						globalBarrierFinishWaitSet.addAll(otherWorkerIds);
						globalBarrierFinishWaitSet.removeAll(globalBarrierFinishPrematureSet);
						globalBarrierFinishPrematureSet.clear();
						globalBarrierSendVerts = startBarrierMsg.getSendQueryChunksList();
						List<ReceiveQueryChunkMessage> recvVerts = startBarrierMsg.getReceiveQueryChunksList();
						globalBarrierRecvVerts = new HashSet<>(recvVerts.size());
						for (ReceiveQueryChunkMessage rvMsg : recvVerts) {
							globalBarrierRecvVerts
							.add(new Pair<>(new HashSet<>(rvMsg.getChunkQueriesList()), rvMsg.getReceiveFromMachine()));
						}
						globalBarrierQuerySupersteps = startBarrierMsg.getQuerySuperstepsMap();
						globalBarrierRequested = true;
					}
					break;


					case Worker_Query_Superstep_Barrier: {
						Q query = deserializeQuery(message.getQueryValues());
						WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(query.QueryId);

						// We have to wait if the query is not already started
						if (activeQuery == null) {
							logger.debug("Postpone barrier message for not yet started query {}", query.QueryId);
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
						int srcWorker = message.getSrcMachine();
						if (globalBarrierStartWaitSet.contains(srcWorker)) globalBarrierStartWaitSet.remove(srcWorker);
						else globalBarrierStartPrematureSet.add(srcWorker);
					}
					return true;
					case Worker_Barrier_Receive_Finished: {
						logger.debug("Worker_Barrier_Finished");
						int srcWorker = message.getSrcMachine();
						if (globalBarrierReceivingFinishWaitSet.contains(srcWorker)) globalBarrierReceivingFinishWaitSet.remove(srcWorker);
						else globalBarrierReceivingFinishPrematureSet.add(srcWorker);
					}
					return true;
					case Worker_Barrier_Finished: {
						logger.debug("Worker_Barrier_Finished");
						int srcWorker = message.getSrcMachine();
						if (globalBarrierFinishWaitSet.contains(srcWorker)) globalBarrierFinishWaitSet.remove(srcWorker);
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

		if (message.getSuperstepQueryExecution() == WorkerQueryExecutionMode.LocalOnThis) {
			// Localmode
			if (message.getSuperstepNo() > activeQuery.getBarrierSyncedSuperstep()) {
				if (activeQuery.BarrierSyncWaitSet.size() == 0) {
					// Received barrier from worker executing local query before received superstepStart from master
					activeQuery.onFinishedLocalmodeSuperstepCompute(message.getSuperstepNo());
					activeQuery.BarrierSyncPostponedList.add(message.getSrcMachine());
					logger.debug("Worker received localOnOther barrier before next superstep {}:{}" + new Object[] {
							activeQuery.Query.QueryId, activeQuery.getMasterStartedSuperstep() });
				}
				else {
					// Already received superstepStart from master
					if (activeQuery.BarrierSyncWaitSet.size() != 1)
						logger.error("Wrong wait set size when localmode: " + activeQuery.BarrierSyncWaitSet.size());

					if (activeQuery.BarrierSyncWaitSet.remove(message.getSrcMachine())) {
						activeQuery.onFinishedLocalmodeSuperstepCompute(message.getSuperstepNo());
					}
					else {
						// Barrier from wrong worker
						logger.error("Received localmode Worker_Superstep_Channel_Barrier from wrong worker: "
								+ activeQuery.Query.QueryId + ":" + activeQuery.getBarrierSyncedSuperstep() + " from "
								+ message.getSrcMachine() + " should be " + activeQuery.BarrierSyncWaitSet);
					}
				}
			}
			else {
				// Completely wrong superstep
				logger.error("Received localmode Worker_Superstep_Channel_Barrier with wrong superstepNo: "
						+ message.getSuperstepNo() + " at " + activeQuery.Query.QueryId + ":"
						+ activeQuery.getBarrierSyncedSuperstep() + " from " + message.getSrcMachine());
			}
		}
		else {
			// No localmode
			if (message.getSuperstepNo() == activeQuery.getBarrierSyncedSuperstep() + 1) {
				if (activeQuery.BarrierSyncWaitSet.remove(message.getSrcMachine())) {
					if (activeQuery.BarrierSyncWaitSet.isEmpty()) {
						activeQuery.onFinishedWorkerSuperstepBarrierSync(activeQuery.getBarrierSyncedSuperstep() + 1);
					}
				}
				else {
					activeQuery.BarrierSyncPostponedList.add(message.getSrcMachine());
				}
			}
			else {
				// Completely wrong superstep
				logger.error("Received Worker_Superstep_Channel_Barrier with wrong superstepNo: "
						+ message.getSuperstepNo() + " at " + activeQuery.Query.QueryId + ":"
						+ activeQuery.getBarrierSyncedSuperstep() + " from " + message.getSrcMachine());
			}
		}

		// Notify master if compute and barrier sync finished
		if (activeQuery.isNextSuperstepLocallyReady()) {
			superstepLocalFinishNotifyMaster(activeQuery);
		}

	}


	/**
	 * Handling for vertices which are moved
	 */
	private void handleVerticesMoving(List<Pair<AbstractVertex<V, E, M, Q>, Integer[]>> verticesMoving, int movedTo) {
		if (verticesMoving.isEmpty()) return;
		List<Integer> vertexMoveIds = verticesMoving.stream().map(v -> v.first.ID)
				.collect(Collectors.toCollection(ArrayList::new));

		// Broadcast vertex invalidate message
		for (int otherWorker : otherWorkerIds) {
			messaging.sendInvalidateRegisteredVerticesMessage(otherWorker, vertexMoveIds, movedTo);
		}

		// Remove vertices registry entries
		remoteVertexMachineRegistry.removeEntries(vertexMoveIds);
	}

	public void handleVertexMessage(VertexMessage<V, E, M, Q> message) {
		WorkerQuery<V, E, M, Q> activeQuery = activeQueries.get(message.queryId);
		if (activeQuery == null) {
			logger.error("Cannot process vertex message, no active query for ID " + message.queryId + ". " + message + " from "
					+ message.srcMachine);
			return;
		}
		int barrierSuperstepNo = activeQuery.getBarrierSyncedSuperstep();

		if (globalBarrierVertexMoveActive) {
			logger.warn("Received vertex message while barrier active: " + activeQuery.QueryId + ":" + message.superstepNo + " from "
					+ message.srcMachine);
		}

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
		if (message.superstepNo == (barrierSuperstepNo + 1)
				|| (message.fromLocalMode && message.superstepNo > barrierSuperstepNo)) {
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
								+ message.srcMachine + " query " + activeQuery.QueryId + ":" + message.superstepNo
								+ " with no redirection");
					}
					activeQuery.QueryLocal.Stats.MessagesReceivedWrongVertex++;
				}
			}
		}
		else {
			logger.error("VertexMessage from wrong barrier superstepNo: " + message.superstepNo + " should be "
					+ (barrierSuperstepNo + 1) + " from " + message.srcMachine + " query " + activeQuery.QueryId);
		}
		message.free(false);
	}


	public void handleGetToKnowMessage(GetToKnowMessage message) {
		for (final Integer srcVertex : message.vertices) {
			if (remoteVertexMachineRegistry.addEntry(srcVertex, message.srcMachine))
				activeQueries.get(message.queryId).QueryLocal.Stats.DiscoveredNewVertexMachines++;
		}
	}


	public void handleUpdateRegisteredVerticesMessage(UpdateRegisteredVerticesMessage message) {
		int updatedEntries;
		if (message.movedTo != ownId)
			updatedEntries = remoteVertexMachineRegistry.updateEntries(message.vertices, message.movedTo);
		else
			updatedEntries = remoteVertexMachineRegistry.removeEntries(message.vertices);
		workerStats.UpdateVertexRegisters += updatedEntries;
	}


	/**
	 * Called after Master_Query_Next_Superstep message.
	 * Prepare next superstep and start if already possible.
	 */
	private void handleMasterNextSuperstep(ControlMessage message) {
		Q msgQuery = deserializeQuery(message.getQueryValues());
		WorkerQuery<V, E, M, Q> query = activeQueries.get(msgQuery.QueryId);
		WorkerQueryExecutionMode queryExecutionMode = message.getStartSuperstep().getWorkerQueryExecution();
		// Indicates if received barrier sync from machine running query in localmode before superstep start
		boolean receivedLocalOnOtherBarrier = (queryExecutionMode == WorkerQueryExecutionMode.LocalOnOther
				&& query.BarrierSyncPostponedList.size() > 0);

		// Checks
		if (query == null) {
			logger.error("Received Master_Next_Superstep for unknown query: " + message);
			return;
		}
		if (!message.hasStartSuperstep()) {
			logger.error("Received Master_Next_Superstep without StartSuperstep information: " + message);
			return;
		}
		if (message.getSuperstepNo() != query.getMasterStartedSuperstep() + 1
				&& !receivedLocalOnOtherBarrier) {
			logger.error("Wrong superstep number to start next: " + query.QueryId + ":" + message.getSuperstepNo()
			+ " localExecution=" + query.localExecution + " executionMode=" + query.getExecutionMode() + "/" + queryExecutionMode
			+ " superstep should be " + (query.getMasterStartedSuperstep() + 1) + ", " + query.getSuperstepNosLog());
			return;
		}

		// Initialize wait set for next barrier, apply all postponed premature barrier syncs
		query.BarrierSyncWaitSet.addAll(message.getStartSuperstep().getWorkersWaitForList());

		if (receivedLocalOnOtherBarrier) {
			for (Integer postponed : new ArrayList<>(query.BarrierSyncPostponedList)) {
				if (query.BarrierSyncWaitSet.remove(postponed)) query.BarrierSyncPostponedList.remove(postponed);
			}
		}
		else {
			for (Integer postponed : query.BarrierSyncPostponedList) {
				if (!query.BarrierSyncWaitSet.remove(postponed))
					logger.error("Postponed worker barrier sync from worker not waiting for: " + postponed + " query " + query.QueryId);
			}
			query.BarrierSyncPostponedList.clear();
		}

		if (query.BarrierSyncWaitSet.isEmpty()) {
			if (!receivedLocalOnOtherBarrier) {
				query.onFinishedWorkerSuperstepBarrierSync(query.getBarrierSyncedSuperstep() + 1);
				// Notify master if compute and barrier sync finished
				if (query.isNextSuperstepLocallyReady()) {
					superstepLocalFinishNotifyMaster(query);
				}
			}
			else {
				logger.trace("Worker received barrier from localcompute before {}:{}",
						new Object[] { query.Query.QueryId, message.getSuperstepNo() });
			}
		}

		// New query stats for next superstep
		query.Query = msgQuery;
		query.QueryLocal = globalValueFactory.createClone(msgQuery);
		workerStats.addQueryStatsstepStats(query.QueryLocal.Stats);
		query.QueryLocal.Stats = new QueryStats();

		if (!receivedLocalOnOtherBarrier) {
			// Finish superstep, start next
			query.onMasterNextSuperstep(message.getSuperstepNo(), queryExecutionMode);
			logger.trace(
					"Worker starting next superstep {}:{}. Active: {}",
					new Object[] { query.Query.QueryId, query.getMasterStartedSuperstep(), query.ActiveVerticesThis.size() });

			// Skip superstep if master says so
			if (queryExecutionMode == WorkerQueryExecutionMode.NonLocalSkip
					|| queryExecutionMode == WorkerQueryExecutionMode.LocalOnOther) {
				skipSuperstepCompute(query);
			}
		}
	}


	/**
	 * Finishes the superstep locally and notifies master.
	 * Called after compute and barrier finished
	 */
	private void superstepLocalFinishNotifyMaster(WorkerQuery<V, E, M, Q> query) {
		// Finish superstep active vertices, prepare for next superstep
		finishQuerySuperstep(query);

		// Notify master that superstep finished
		sendMasterSuperstepFinished(query);
		query.onLocalFinishSuperstep(query.getMasterStartedSuperstep());

		logger.trace(
				"Worker finished local superstep {}:{}. Active: {}",
				new Object[] { query.Query.QueryId, query.getMasterStartedSuperstep(), query.ActiveVerticesThis.size() });
	}

	private void finishQuerySuperstep(WorkerQuery<V, E, M, Q> query) {
		long startTime = System.nanoTime();
		Int2ObjectMap<AbstractVertex<V, E, M, Q>> swap = query.ActiveVerticesThis;
		query.ActiveVerticesThis = query.ActiveVerticesNext;
		query.ActiveVerticesNext = swap;
		query.ActiveVerticesNext.clear();
		query.VerticesEverActive.addAll(query.ActiveVerticesThis.keySet());

		// Prepare active vertices, now we have all vertex messages and compute is finished
		Integer queryId = query.Query.QueryId;
		for (AbstractVertex<V, E, M, Q> vert : query.ActiveVerticesThis.values()) {
			vert.prepareForNextSuperstep(queryId);
		}

		// Reset active vertices
		query.QueryLocal.setActiveVertices(query.ActiveVerticesThis.size());

		// Finish barrier
		query.QueryLocal.Stats.StepFinishTime += System.nanoTime() - startTime;
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
						ownId, workerQuery.Query, workerQuery.getExecutionMode()),
				true);
	}

	private void sendMasterInitialized() {
		messaging.sendControlMessageUnicast(masterId, ControlMessageBuildUtil.Build_Worker_Initialized(ownId, localVertices.size()),
				true);
	}

	private void sendMasterSuperstepFinished(WorkerQuery<V, E, M, Q> workerQuery) {
		messaging.sendControlMessageUnicast(masterId,
				ControlMessageBuildUtil.Build_Worker_QuerySuperstepFinished(workerQuery.getMasterStartedSuperstep(), ownId,
						workerQuery.QueryLocal, workerStatsSamplesToSend),
				true);
		//logger.info("** " + workerQuery.QueryId + ":" + workerQuery.getMasterStartedSuperstep() + " " + workerQuery.getExecutionMode());
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

			// Local mode will be stopped as soon as remote messages are sent
			if (query.localExecution) {
				logger.trace("Localmode ending after sending remote message");
			}
			query.localExecution = false;

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
		assert query.getExecutionMode() == WorkerQueryExecutionMode.NonLocal
				|| query.getExecutionMode() == WorkerQueryExecutionMode.LocalOnThis;
		final List<Pair<Integer, M>> msgList = packVertexMessage(msgBucket);
		messaging.sendVertexMessageUnicast(dstMachine, superstepNo, ownId, query.QueryId,
				query.getExecutionMode() == WorkerQueryExecutionMode.LocalOnThis,
				msgList);
		query.QueryLocal.Stats.MessageBucketsSentUnicast++;
	}

	private void sendBroadcastVertexMessageBucket(WorkerQuery<V, E, M, Q> query, int superstepNo) {
		assert query.getExecutionMode() == WorkerQueryExecutionMode.NonLocal
				|| query.getExecutionMode() == WorkerQueryExecutionMode.LocalOnThis;
		if (vertexMessageBroadcastBucket.messages.isEmpty()) return;
		final List<Pair<Integer, M>> msgList = packVertexMessage(vertexMessageBroadcastBucket);
		messaging.sendVertexMessageBroadcast(otherWorkerIds, superstepNo, ownId, query.QueryId,
				query.getExecutionMode() == WorkerQueryExecutionMode.LocalOnThis,
				msgList);
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
	private void sendQueryVerticesToMove(Set<Integer> queryChunk, int sendToWorker, int maxVertices) {
		long startTime = System.nanoTime();
		int verticesMessagesSent = 0;
		int verticesToMove = 0;
		int verticesSent = 0;
		int verticesNotSent = 0;

		if (queryChunk.isEmpty()) {
			logger.error("Cannot send vertices for empty queryChunk set");
			return;
		}

		// Remove queries from chunk set that are not active anymore or cant be moved
		Set<Integer> chunkQueries = new HashSet<>();
		WorkerQuery<V, E, M, Q> smallestQuery = null;
		for (Integer qId : queryChunk) {
			WorkerQuery<V, E, M, Q> q = queriesHistoryMap.get(qId);
			if (q != null) {
				if (q.ActiveVerticesNext.isEmpty()) {
					if (smallestQuery == null || q.VerticesEverActive.size() < smallestQuery.VerticesEverActive.size())
						smallestQuery = q;
					chunkQueries.add(qId);
				}
				else {
					// Dont send vertices
					logger.warn(
							"Cant move vertecies of query, has ActiveVerticesNext " + q.QueryId + ":" + q.getMasterStartedSuperstep());
					messaging.sendMoveVerticesMessage(sendToWorker, queryChunk, new ArrayList<>(), true);
					return;
				}
			}
		}
		if (chunkQueries.isEmpty()) {
			logger.debug("Vertices not moved because all chunk queries inactive: {}", queryChunk);
			messaging.sendMoveVerticesMessage(sendToWorker, queryChunk, new ArrayList<>(), true);
			return;
		}
		assert smallestQuery != null;

		Map<Integer, WorkerQuery<V, E, M, Q>> nonChunkQueries = new HashMap<>();
		for (Entry<Integer, WorkerQuery<V, E, M, Q>> qKv : queriesHistoryMap.entrySet()) {
			if (!chunkQueries.contains(qKv.getKey())) {
				nonChunkQueries.put(qKv.getKey(), qKv.getValue());
			}
		}

		// Find query move vertices
		//		IntSet removeBuffer = new IntOpenHashSet();
		// Only vertices that are active in query-chunks
		//		IntSet moveVerticesCandidates = new IntOpenHashSet(smallestQuery.VerticesEverActive);
		//		for (Integer qId : chunkQueries) {
		//			if (qId.equals(smallestQuery.QueryId)) continue;
		//			WorkerQuery<V, E, M, Q> q = queriesHistoryMap.get(qId);
		//			removeBuffer.clear();
		//			for (IntIterator it = moveVerticesCandidates.iterator(); it.hasNext();) {
		//				int next = it.nextInt();
		//				if (!q.VerticesEverActive.contains(next))
		//					removeBuffer.add(next);
		//			}
		//			moveVerticesCandidates.removeAll(removeBuffer);
		//		}

		IntSet moveVerticesCandidates = new IntOpenHashSet();
		for (Integer qId : chunkQueries) {
			WorkerQuery<V, E, M, Q> q = queriesHistoryMap.get(qId);
			moveVerticesCandidates.addAll(q.VerticesEverActive);
		}

		// Only vertices that are not active in non-query-chunks
		IntSet moveVertices = new IntOpenHashSet();
		for (IntIterator it = moveVerticesCandidates.iterator(); it.hasNext();) {
			int next = it.nextInt();
			boolean isInChunk = true;
			for (Entry<Integer, WorkerQuery<V, E, M, Q>> qKv : nonChunkQueries.entrySet()) {
				if (qKv.getValue().VerticesEverActive.contains(next)) {
					isInChunk = false;
					break;
				}
			}
			if (isInChunk)
				moveVertices.add(next);
		}

		// Move vertices
		List<Integer> vertexActiveQueriesBuffer = new ArrayList<>();
		List<Pair<AbstractVertex<V, E, M, Q>, Integer[]>> verticesToSend = new ArrayList<>();
		for (IntIterator it = moveVertices.iterator(); it.hasNext();) {
			int moveVertexId = it.nextInt();
			AbstractVertex<V, E, M, Q> moveVertex = localVertices.get(moveVertexId);
			if (moveVertex == null) {
				// Vertex to move not found here anymore
				verticesNotSent++;
				continue;
			}
			if (verticesToMove >= maxVertices) {
				// Reached max move limit
				verticesNotSent++;
				continue;
			}

			// Remove vertex
			localVertices.remove(moveVertexId);
			vertexActiveQueriesBuffer.clear();
			for (WorkerQuery<V, E, M, Q> q : activeQueries.values()) {
				if (q.ActiveVerticesThis.containsKey(moveVertexId)) {
					q.ActiveVerticesThis.remove(moveVertexId);
					q.VerticesEverActive.remove(moveVertexId);
					vertexActiveQueriesBuffer.add(q.QueryId);
					// Reset local query locality stat
					queriesLocalSupersteps.remove(q.QueryId);
				}
			}

			verticesToMove++;
			verticesToSend.add(new Pair<>(moveVertex, vertexActiveQueriesBuffer.toArray(new Integer[0])));

			// Send vertices now if bucket full
			if (verticesToSend.size() >= Configuration.VERTEX_MOVE_BUCKET_MAX_VERTICES) {
				handleVerticesMoving(verticesToSend, sendToWorker);
				messaging.sendMoveVerticesMessage(sendToWorker, queryChunk, verticesToSend, false);
				verticesSent += verticesToSend.size();
				verticesToSend = new ArrayList<>();
			}
			if (Configuration.DETAILED_STATS) {
				verticesMessagesSent += moveVertex.getBufferedMessageCount();
			}
		}


		// Send remaining vertices (or empty message if none)
		handleVerticesMoving(verticesToSend, sendToWorker);
		messaging.sendMoveVerticesMessage(sendToWorker, queryChunk, verticesToSend, true);
		verticesSent += verticesToSend.size();

		verticesToMove += verticesToSend.size();
		logger.info("Sent {} and skipped {} to {} for query chunk {}",
				new Object[] { verticesToMove, verticesNotSent, sendToWorker, queryChunk }); // TODO Debug

		workerStats.MoveSendVertices += verticesSent;
		workerStats.MoveSendVerticesTime += (System.nanoTime() - startTime);
		workerStats.MoveSendVerticesMessages += verticesMessagesSent;
	}

	/**
	 * Handles incoming messages, dont process, only queue
	 */
	public void handleIncomingMoveVerticesMessage(MoveVerticesMessage<V, E, M, Q> message) {

		if (message.lastSegment) {
			// Remove from globalBarrierRecvVerts if received all vertices
			if (!globalBarrierRecvVerts.remove(new Pair<>(message.chunkQueries, message.srcMachine))) {
				logger.error("TODO Premature globalBarrierRecvVerts not implemented");
			}
		}

		queuedMoveMessages.add(message);
	}

	/**
	 * Processes queued move messages
	 */
	public void processQueuedMoveVerticesMessages() {
		long startTime = System.nanoTime();

		for (MoveVerticesMessage<V, E, M, Q> message : queuedMoveMessages) {
			for (Pair<AbstractVertex<V, E, M, Q>, Integer[]> movedVertInfo : message.vertices) {
				Pair<AbstractVertex<V, E, M, Q>, Integer[]> movedVertPair = movedVertInfo;
				AbstractVertex<V, E, M, Q> movedVert = movedVertPair.first;
				for (Integer queryActiveInId : movedVertPair.second) {
					WorkerQuery<V, E, M, Q> queryActiveIn = activeQueries.get(queryActiveInId);
					if (queryActiveIn != null) {
						queryActiveIn.ActiveVerticesThis.put(movedVert.ID, movedVert);
						queryActiveIn.VerticesEverActive.add(movedVert.ID);
						// Reset local query locality stat
						queriesLocalSupersteps.remove(queryActiveIn.QueryId);
					}
					//					else {
					//						logger.warn("Received vertex for unknown query: " + queryActiveInId);
					//					}
				}
				localVertices.put(movedVert.ID, movedVert);
			}
			workerStats.MoveRecvVertices += message.vertices.size();
		}

		queuedMoveMessages.clear();
		workerStats.MoveRecvVerticesTime += (System.nanoTime() - startTime);
	}



	// #################### Others #################### //

	private void sampleWorkerStats() {
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

		// Worker stats active vertices
		long activeVertices = 0;
		for (WorkerQuery<V, E, M, Q> query : activeQueries.values())
			activeVertices += query.ActiveVerticesThis.size();
		workerStats.ActiveVertices = activeVertices;
		workerStats.WorkerVertices = localVertices.size();

		// TODO Rather slow
		int activeVertsTimeWindow = 60000; // TODO Config
		long timeNow = System.currentTimeMillis();
		long windowLimit = timeNow - activeVertsTimeWindow;
		long windowActiveVerts = 0;
		for (AbstractVertex<V, E, M, Q> vert : localVertices.values()) {
			if (vert.lastSuperstepTime >= windowLimit) windowActiveVerts++;
		}
		workerStats.ActiveVerticesTimeWindow = windowActiveVerts;
		System.err.println("ActiveVerticesTimeWindow in " + (System.currentTimeMillis() - timeNow));

		try {
			workerStatsSamplesToSend.add(workerStats.getSample(System.currentTimeMillis() - masterStartTime));
		}
		catch (Exception e) {
			logger.error("Failure at sample worker stats", e);
		}
		workerStats = new WorkerStats();
	}

	private Map<IntSet, Integer> calculateQueryIntersectChunksSampled(int samplingFactor) {
		long startTimeMs = System.currentTimeMillis();
		long startTimeNano = System.nanoTime();
		Random rd = new Random(0);

		// Remove older and inactive queries from query cut
		for (int i = 0; i < queriesHistoryList.size(); i++) {
			WorkerQuery<V, E, M, Q> query = queriesHistoryList.get(i);
			double qlocalSuperstepRatio = getQueryLocalSuperstepRatio(query.QueryId);
			long qAge = (startTimeMs - query.startTime);
			if (qAge > Configuration.QUERY_CUT_TIME_WINDOW) {
				//				if (qlocalSuperstepRatio < 0.8) { // TODO Test
				System.err.println("REM " + query.QueryId + " " + qlocalSuperstepRatio);
				queriesHistoryList.remove(i);
				queriesHistoryMap.remove(query.QueryId);
				queriesLocalSupersteps.remove(query.QueryId);
				i--;
				//				}
				//				else {
				//					System.err.println("KEEP " + query.QueryId + " " + qlocalSuperstepRatio);
				//				}
			}
		}
		// Remove older if to many queries
		for (int i = 0; queriesHistoryList.size() > Configuration.QUERY_CUT_MAX_QUERIES && i < queriesHistoryList.size(); i++) {
			WorkerQuery<V, E, M, Q> query = queriesHistoryList.get(i);
			double qlocalSuperstepRatio = getQueryLocalSuperstepRatio(query.QueryId);
			//			if (qlocalSuperstepRatio < 0.8) { // TODO Test
			System.err.println("REM2 " + query.QueryId + " " + qlocalSuperstepRatio);
			queriesHistoryList.remove(i);
			i--;
			queriesHistoryMap.remove(query.QueryId);
			queriesLocalSupersteps.remove(query.QueryId);
			//			}
			//			else {
			//				System.err.println("KEEP2 " + query.QueryId + " " + qlocalSuperstepRatio);
			//			}
		}

		//long start2 = System.currentTimeMillis();
		Map<IntSet, Integer> intersectChunks = new HashMap<>();

		// Find all active vertics
		IntSet allActiveVertices = new IntOpenHashSet();
		for (WorkerQuery<V, E, M, Q> query : queriesHistoryMap.values()) {
			allActiveVertices.addAll(query.VerticesEverActive);
		}

		// Find queries vertices are active in
		IntSet vertexQueriesSet = new IntOpenHashSet();
		for (IntIterator it = allActiveVertices.iterator(); it.hasNext();) {
			int vertex = it.nextInt();
			if (samplingFactor > 1 && rd.nextInt(samplingFactor) != 0) continue;

			for (WorkerQuery<V, E, M, Q> query : queriesHistoryMap.values()) {
				if (query.VerticesEverActive.contains(vertex))
					vertexQueriesSet.add(query.QueryId);
			}

			Integer countNow = intersectChunks.get(vertexQueriesSet);
			if (countNow != null) {
				intersectChunks.put(vertexQueriesSet, countNow + 1);
			}
			else {
				intersectChunks.put(new IntOpenHashSet(vertexQueriesSet), 1);
			}
			vertexQueriesSet.clear();
		}

		// Limit chunk sizes, merge chunks
		//		int maxChunkQueries = 3; // TODO Config
		//		List<Integer> chunkQueriesBuffer = new ArrayList<>();
		//		for (IntSet chunk : new ArrayList<>(intersectChunks.keySet())) {
		//			if (chunk.size() > maxChunkQueries) {
		//				chunkQueriesBuffer.clear();
		//				chunkQueriesBuffer.addAll(chunk);
		//				Collections.sort(chunkQueriesBuffer);
		//				IntSet reducedChunk = new IntOpenHashSet(chunkQueriesBuffer.subList(0, maxChunkQueries));
		//				int chunkSize = intersectChunks.remove(chunk);
		//				intersectChunks.put(reducedChunk, MiscUtil.defaultInt(intersectChunks.get(reducedChunk)) + chunkSize);
		//			}
		//		}

		// Sort out neglegible small chunks and multiply with samplingFactor
		for (IntSet chunk : new ArrayList<>(intersectChunks.keySet())) {
			int chunkSize = intersectChunks.get(chunk) * samplingFactor;
			if (chunkSize < Configuration.QUERY_CUT_CHUNK_MIN_SIZE) {
				intersectChunks.remove(chunk);
			}
			else {
				intersectChunks.put(chunk, chunkSize);
			}
		}

		// Limit chunks
		Map<IntSet, Integer> limitedIntersectChunks = intersectChunks.entrySet().stream()
				.limit(200) // TODO Config
				.sorted(Map.Entry.<IntSet, Integer>comparingByValue().reversed())
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
						(e1, e2) -> e1, LinkedHashMap::new));

		workerStats.IntersectCalcTime += System.nanoTime() - startTimeNano;
		logger.info("Sampled IntersectCalcTime {}ms", (System.currentTimeMillis() - startTimeMs)); // TODO debug

		return limitedIntersectChunks;
	}


	private double getQueryLocalSuperstepRatio(int queryId) {
		Pair<Integer, Integer> superstepStat = queriesLocalSupersteps.get(queryId);
		if (superstepStat == null) return 0;
		return (double) superstepStat.second / superstepStat.first;
	}

	@Override
	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}

	private Q deserializeQuery(ByteString bytes) {
		return globalValueFactory.createFromBytes(ByteBuffer.wrap(bytes.toByteArray()));
	}
}
