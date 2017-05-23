package mthesis.concurrent_graph.master;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.apps.shortestpath.SPQuery;
import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.QueryVertexChunksMapMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.QueryVertexChunksMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.Messages.WorkerQueryExecutionMode;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.logging.ErrWarnCounter;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.master.vertexmove.ILSVertexMoveDecider;
import mthesis.concurrent_graph.master.vertexmove.VertexMoveDeciderService;
import mthesis.concurrent_graph.master.vertexmove.VertexMoveDecision;
import mthesis.concurrent_graph.plotting.JFreeChartPlotter;
import mthesis.concurrent_graph.util.FileUtil;
import mthesis.concurrent_graph.util.MiscUtil;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.worker.WorkerStats;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Concurrent graph processing master main
 *
 * @author Jonas Grunert
 *
 * @param <Q>
 *            Global query values
 */
public class MasterMachine<Q extends BaseQuery> extends AbstractMachine<NullWritable, NullWritable, NullWritable, Q> {

	private long firstQueryStartTimeMs = -1;
	private long firstQueryStartTimeNano = -1;
	private long masterStartTimeMs;
	private long masterStartTimeNano;
	private final List<Integer> workerIds;
	private final Set<Integer> workersToInitialize;
	private Map<Integer, MasterQuery<Q>> activeQueries = new HashMap<>();
	private int queuedQueries = 0;

	private static final boolean skipInactiveWorkers = Configuration.getPropertyBoolDefault("SkipInactiveWorkers", true);
	private static final boolean localQueryExecution = Configuration.getPropertyBoolDefault("LocalQueryExecution", true);
	private static final boolean queryGlobalBarrier = Configuration.getPropertyBoolDefault("QueryGlobalBarrier", false);
	private static final boolean VertexMoveEnabled = Configuration.getPropertyBoolDefault("VertexMoveEnabled", false);

	private final boolean EnableQueryStats = Configuration.getPropertyBoolDefault("EnableQueryStats", true);
	private final long LocalSuperstepTimeWindow = Configuration.getPropertyLongDefault("LocalSuperstepTimeWindow", 40000);
	private final double LsruThresholdLow = Configuration.getPropertyDoubleDefault("LsruThresholdLow", 0.3);
	private final double LsruDeltaThresholdNeg = Configuration.getPropertyDoubleDefault("LsruDeltaThresholdNeg", 0.3);
	private final double LsruDeltaThresholdPos = Configuration.getPropertyDoubleDefault("LsruDeltaThresholdPos", 0.3);
	private final int LsruExtraShots = Configuration.getPropertyIntDefault("LsruExtraShots", 3);
	private final double VertexMaxActVertsImbalance = Configuration.getPropertyDoubleDefault("VertexMaxActVertsImbalance", 0.3);
	private final double VertexAvgActVertsImbalance = Configuration.getPropertyDoubleDefault("VertexAvgActVertsImbalance", 0.2);
	private final double VertexMaxActVertsImbTrigger = Configuration.getPropertyDoubleDefault("VertexMaxActVertsImbTrigger", 0.10);
	private final double VertexAvgActVertsImbTrigger = Configuration.getPropertyDoubleDefault("VertexAvgActVertsImbTrigger", 0.05);

	// Query logging for later evaluation
	private final String queryStatsDir;
	private final Map<Integer, List<SortedMap<Integer, Q>>> queryStatsStepMachines = new HashMap<>();
	private final Map<Integer, List<Q>> queryStatsSteps = new HashMap<>();
	private final Map<Integer, List<Long>> queryStatsStepTimes = new HashMap<>();
	private final Map<Integer, Q> queryStatsTotals = new HashMap<>();
	private final Map<Integer, Long> queryStartTimes = new HashMap<>();
	private final Map<Integer, Long> queryDurations = new HashMap<>();
	private long finishedTimeSinceStart;
	private long finishedTimeSinceFirstQuery;

	// Map WorkerId->(timestamp, workerStatsSample)
	private Map<Integer, List<Pair<Long, WorkerStats>>> workerStats = new HashMap<>();
	// Map WorkerId->(Map Queries->NumVertices)
	private Map<Integer, Map<IntSet, Integer>> workerQueryChunks = new HashMap<>();

	private final String inputFile;
	private final String inputPartitionDir;
	private final String outputDir;
	private int vertexCount;
	private final MasterInputPartitioner inputPartitioner;
	private final MasterOutputEvaluator<Q> outputCombiner;
	private final BaseQueryGlobalValuesFactory<Q> queryValueFactory;

	private final BlockingQueue<ChannelMessage> messageQueue = new LinkedBlockingQueue<>();
	/** Indicates if global barrier should be active as soon as all queries in standby */
	private volatile boolean globalBarrierPlanned = false;
	private final Set<Integer> globalBarrierWaitSet = new HashSet<>();
	/** Queries that are ready for the next superstep.
	 *  The next superstep wil be started once all active queries are ready. */
	private final Set<MasterQuery<Q>> queriesReadyForNextStep = new HashSet<>();
	private VertexMoveDecision moveDecission = null;

	private final VertexMoveDeciderService vertexMoveDeciderService;

	private final Map<Integer, Long> latestWorkerTotalVertices = new HashMap<>();
	private final Map<Integer, Long> latestWorkerActiveVerticesWindows = new HashMap<>();
	private double latestWavwMaxImbalance = 0;
	private double latestWavwAvgImbalance = 0;
	private double localSuperstepsRatioUnique = 0;
	private double lastQMoveLSRU = 0;
	private int remainingLsruExtraShots = 1;

	private static final boolean SearchNextTagTestMode = Configuration.getPropertyBoolDefault("SearchNextTagTestMode", false);
	private static final int SearchNextTagNumTags = Configuration.getPropertyIntDefault("SearchNextTagNumTags", 100);
	private Random nextTagRestRandom = new Random(0);


	public MasterMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, String inputFile,
			String inputPartitionDir, MasterInputPartitioner inputPartitioner, MasterOutputEvaluator<Q> outputCombiner,
			String outputDir, BaseQueryGlobalValuesFactory<Q> globalValueFactory) {
		super(machines, ownId, null);
		this.workerIds = workerIds;
		this.workersToInitialize = new HashSet<>(workerIds);
		this.vertexCount = 0;
		this.inputFile = inputFile;
		this.inputPartitionDir = inputPartitionDir;
		this.inputPartitioner = inputPartitioner;
		this.outputCombiner = outputCombiner;
		this.outputDir = outputDir;
		this.queryStatsDir = outputDir + File.separator + "stats";
		this.queryValueFactory = globalValueFactory;
		this.vertexMoveDeciderService = new VertexMoveDeciderService(new ILSVertexMoveDecider(), workerIds);
		FileUtil.createDirOrEmptyFiles(outputDir);
		FileUtil.createDirOrEmptyFiles(queryStatsDir);
		saveSetupSummary(machines, ownId);
		saveConfigSummary();

		for (Integer workerId : workerIds) {
			workerStats.put(workerId, new ArrayList<>());
			workerQueryChunks.put(workerId, new HashMap<>());
		}
	}

	@Override
	public void start() {
		masterStartTimeMs = System.currentTimeMillis();
		masterStartTimeNano = System.nanoTime();
		super.start();
		// Initialize workers
		initializeWorkersAssignPartitions(); // Signal workers to initialize
		logger.info("Workers partitions assigned and initialize starting after "
				+ ((System.nanoTime() - masterStartTimeNano) / 1000000) + "ms");
		if (Configuration.VERTEX_BARRIER_MOVE_ENABLED)
			vertexMoveDeciderService.start();
	}


	@Override
	public void run() {
		while (!getStopRequested() && !Thread.interrupted()) {
			try {
				ChannelMessage message = messageQueue.take();
				handleMessage(message);
			}
			catch (InterruptedException e) {
				if (!getStopRequested())
					logger.error("interrupt", e);
			}
		}
	}



	/**
	 * Starts a new query on this master and its workers
	 */
	public synchronized void startQuery(Q query) {
		// Get query ready
		if (activeQueries.containsKey(query.QueryId))
			throw new RuntimeException("There is already an active query with this ID: " + query.QueryId);

		if (SearchNextTagTestMode) {
			((SPQuery) query).Tag = nextTagRestRandom.nextInt(SearchNextTagNumTags);
			((SPQuery) query).To = -1;
		}

		logger.info("Start request for query: " + query.QueryId);
		if (!workersToInitialize.isEmpty()) {
			logger.info("Wait for workersInitialized before starting query: " + query.QueryId);
			while (!workersToInitialize.isEmpty()) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		long startDelay = Configuration.getPropertyLongDefault("QueryStartDelay", 100);
		if (startDelay > 0) {
			try {
				Thread.sleep(startDelay);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		if (EnableQueryStats) {
			queryStatsStepMachines.put(query.QueryId, new ArrayList<>());
			queryStatsSteps.put(query.QueryId, new ArrayList<>());
			queryStatsStepTimes.put(query.QueryId, new ArrayList<>());
		}

		if (getActiveQueryCount() >= Configuration.MAX_PARALLEL_QUERIES) {
			logger.info("Wait for activeQueries<MAX_PARALLEL_QUERIES " + Configuration.MAX_PARALLEL_QUERIES
					+ " before starting query: " + query.QueryId);
			while (getActiveQueryCount() >= Configuration.MAX_PARALLEL_QUERIES) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					logger.error("interrupt", e);
					throw new RuntimeException(e);
				}
			}
		}

		synchronized (activeQueries) {
			queuedQueries++;
		}
		messageQueue.add(new StartQueryMessage<BaseQuery>(query));
	}

	private int getActiveQueryCount() {
		synchronized (activeQueries) {
			return activeQueries.size() + queuedQueries;
		}
	}

	public boolean isQueryActive(int queryId) {
		synchronized (activeQueries) {
			return activeQueries.containsKey(queryId);
		}
	}

	public void waitForQueryFinish(int queryId) {
		while (isQueryActive(queryId)) {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				logger.info("waitForQueryFinish interrupted");
				return;
			}
		}
	}

	public void waitForAllQueriesFinish() {
		while (getActiveQueryCount() != 0) {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				logger.info("waitForAllQueriesFinish interrupted");
				return;
			}
		}
	}


	@Override
	public void onIncomingMessage(ChannelMessage message) {
		messageQueue.add(message);
	}

	@SuppressWarnings("unchecked")
	public void handleMessage(ChannelMessage message) {
		if (message.getTypeCode() == StartQueryMessage.ChannelMessageTypeCode) {
			if (firstQueryStartTimeMs == -1) {
				logger.info("First query started after " + (System.currentTimeMillis() - masterStartTimeMs));
				firstQueryStartTimeMs = System.currentTimeMillis();
				firstQueryStartTimeNano = System.nanoTime();
			}

			handleStartQuery(((StartQueryMessage<Q>) message).Query);
			return;
		}

		if (message.getTypeCode() != 1) {
			logger.error("Master machine can only handle ProtoEnvelopeMessage: " + message);
			logger.error("Communication error, shutting down.");
			stop();
			return;
		}
		MessageEnvelope protoMsg = ((ProtoEnvelopeMessage) message).message;
		if (!protoMsg.hasControlMessage()) {
			logger.error("Master machine can only handle ProtoEnvelopeMessage with ControlMessage: " + message);
			logger.error("Communication error, shutting down.");
			stop();
			return;
		}
		ControlMessage controlMsg = protoMsg.getControlMessage();

		int srcMachine = controlMsg.getSrcMachine();

		// Process query
		if (controlMsg.getType() == ControlMessageType.Worker_Initialized) {
			// Process Worker_Initialized message
			if (workersToInitialize.isEmpty()) {
				logger.error("Received Worker_Initialized but all workers intitialized");
				return;
			}

			workersToInitialize.remove(srcMachine);
			vertexCount += controlMsg.getWorkerInitialized().getVertexCount();
			logger.debug("Worker initialized: {}", srcMachine);

			if (workersToInitialize.isEmpty()) {
				logger.info("All workers initialized, " + workerIds.size() + " workers, " + vertexCount
						+ " vertices after " + ((System.nanoTime() - masterStartTimeNano) / 1000000) + "ms");
			}
		}
		else if (controlMsg.getType() == ControlMessageType.Worker_Query_Superstep_Finished
				|| controlMsg.getType() == ControlMessageType.Worker_Query_Finished) {
			// Process worker query message
			if (!workersToInitialize.isEmpty()) {
				logger.error("Received non-Worker_Initialized but not all workers intitialized");
				return;
			}

			// Get message query
			if (controlMsg.getQueryValues().size() == 0)
				throw new RuntimeException("Control message without query: " + message);
			Q msgQueryOnWorker = queryValueFactory
					.createFromBytes(ByteBuffer.wrap(controlMsg.getQueryValues().toByteArray()));
			MasterQuery<Q> msgActiveQuery = activeQueries.get(msgQueryOnWorker.QueryId);
			if (msgActiveQuery == null)
				throw new RuntimeException("Control message without ungknown query: " + message);

			// Check superstep
			if (controlMsg.getSuperstepNo() != msgActiveQuery.StartedSuperstepNo) {
				if (msgActiveQuery.IsInLocalMode) {
					msgActiveQuery.setSuperstepAfterLocalExecution(controlMsg.getSuperstepNo());
					logger.debug("Query back from local execution {}:{}, message from {}", new Object[] {
							msgQueryOnWorker.QueryId, msgActiveQuery.StartedSuperstepNo, controlMsg.getSrcMachine() });
				}
				else {
					logger.error("Message for wrong superstep. not {}:{} {}",
							new Object[] { msgActiveQuery.BaseQuery.QueryId, msgActiveQuery.StartedSuperstepNo, message });
					return;
				}
			}

			// Check if a worker waiting for
			if (!msgActiveQuery.workersWaitingFor.contains(srcMachine)) {
				logger.error("Query " + msgQueryOnWorker.QueryId + " not waiting for " + msgQueryOnWorker);
				return;
			}
			msgActiveQuery.workersWaitingFor.remove(srcMachine);

			// Get latest worker stats
			if (controlMsg.hasWorkerStats()) {
				List<WorkerStatSample> samples = controlMsg.getWorkerStats().getSamplesList();
				if (samples.size() > 0) {
					for (WorkerStatSample sample : samples) {
						WorkerStats stats = new WorkerStats(sample.getStatsBytes());
						workerStats.get(controlMsg.getSrcMachine()).add(new Pair<Long, WorkerStats>(sample.getTime(), stats));
						latestWorkerTotalVertices.put(controlMsg.getSrcMachine(), stats.WorkerVertices);
						latestWorkerActiveVerticesWindows.put(controlMsg.getSrcMachine(), stats.ActiveVerticesTimeWindow);
					}

					// Update local supersteps stat
					double lsrSupersteps = 0;
					double lsrLocalSupersteps = 0;
					long lsrTimeWindow = System.currentTimeMillis() - masterStartTimeMs - LocalSuperstepTimeWindow;
					for (List<Pair<Long, WorkerStats>> workerSamples : workerStats.values()) {
						for (Pair<Long, WorkerStats> stat : workerSamples) {
							if (stat.first >= lsrTimeWindow) {
								lsrSupersteps += stat.second.getSuperstepsComputed();
								lsrLocalSupersteps += stat.second.getLocalSuperstepsComputed();
							}
						}
					}
					double lsrNonlocalSuperstepsUnique = (lsrSupersteps - lsrLocalSupersteps) / workerIds.size();
					double lsrSuperstepsUnique = lsrNonlocalSuperstepsUnique + lsrLocalSupersteps;
					localSuperstepsRatioUnique = lsrSupersteps > 0 ? (double) lsrLocalSupersteps / lsrSuperstepsUnique : 0;

					// Update imbalance stat
					long avgVerts = 0;
					for (Long workerVerts : latestWorkerActiveVerticesWindows.values()) {
						avgVerts += workerVerts;
					}
					avgVerts /= latestWorkerActiveVerticesWindows.size();
					double avgImbalance = 0;
					double maxImbalance = 0;
					if (avgVerts > 0) {
						for (Long workerVerts : latestWorkerActiveVerticesWindows.values()) {
							double imbalance = (double) Math.abs(workerVerts - avgVerts) / avgVerts;
							avgImbalance += imbalance;
							maxImbalance = Math.max(maxImbalance, imbalance);
						}
						avgImbalance /= latestWorkerActiveVerticesWindows.size();
						latestWavwMaxImbalance = maxImbalance;
						latestWavwAvgImbalance = avgImbalance;
					}
				}
			}

			if (controlMsg.getType() == ControlMessageType.Worker_Query_Superstep_Finished) {
				if (!msgActiveQuery.IsComputing) {
					logger.error(
							"Query " + msgQueryOnWorker.QueryId + " not computing, wrong message: " + msgQueryOnWorker);
					return;
				}

				msgActiveQuery.aggregateQuery(msgQueryOnWorker, controlMsg.getSrcMachine());

				// Log worker superstep stats
				if (EnableQueryStats && controlMsg.getSuperstepNo() >= 0) {
					List<SortedMap<Integer, Q>> queryStepList = queryStatsStepMachines.get(msgQueryOnWorker.QueryId);
					if (queryStepList != null) {
						SortedMap<Integer, Q> queryStepWorkerMap;
						if (queryStepList.size() <= controlMsg.getSuperstepNo()) {
							queryStepWorkerMap = new TreeMap<>();
							queryStepList.add(queryStepWorkerMap);
						}
						else {
							queryStepWorkerMap = queryStepList.get(controlMsg.getSuperstepNo());
						}
						queryStepWorkerMap.put(srcMachine, msgQueryOnWorker);
					}
				}

				// Check if all workers finished superstep
				if (msgActiveQuery.workersWaitingFor.isEmpty()) {
					// Correct/wrong message count should match unicast+broadcast message count, otherwise there might be communication errors.
					// TODO Investigate why metrics are wrong. Results are correct but metrics falsely indicate errors
					//					long msgsReceivedWrong = msgActiveQuery.QueryStepAggregator.Stats.MessagesReceivedWrongVertex;
					//					long msgsReceivedCorrect = msgActiveQuery.QueryStepAggregator.Stats.MessagesReceivedCorrectVertex;
					//					long msgsSentBroadcast = msgActiveQuery.QueryStepAggregator.Stats.MessagesSentBroadcast;
					//					long msgsSentUnicast = msgActiveQuery.QueryStepAggregator.Stats.MessagesSentUnicast;
					//					long msgsExpectedWrongBroadcast = msgActiveQuery.QueryStepAggregator.Stats.MessagesSentBroadcast
					//							/ (workerIds.size() - 1) * (workerIds.size() - 2);
					//					long msgsExpectedCorrectBroadcast = msgsSentBroadcast - msgsExpectedWrongBroadcast;
					//					long msgsExpectedCorrect = msgsSentUnicast + msgsExpectedCorrectBroadcast;
					//					if (workerIds.size() > 1 && msgsReceivedWrong != msgsExpectedWrongBroadcast) {
					//						logger.warn(msgActiveQuery.BaseQuery.QueryId + ":" + msgActiveQuery.SuperstepNo + " " +
					//								String.format(
					//										"Unexpected wrong vertex message count is %d but should be %d. Does not match broadcast message count %d. Possible communication errors.",
					//										msgsReceivedWrong, msgsExpectedWrongBroadcast, msgsSentBroadcast));
					//					}
					//					if (workerIds.size() > 1 && msgsReceivedCorrect != msgsExpectedCorrect) {
					//						logger.warn(msgActiveQuery.BaseQuery.QueryId + ":" + msgActiveQuery.SuperstepNo + " " +
					//								String.format(
					//										"Unexpected correct vertex message count is %d but should be %d (%d+%d). Possible communication errors.",
					//										msgsReceivedCorrect, msgsExpectedCorrect,
					//										msgsSentUnicast, msgsExpectedCorrectBroadcast));
					//					}

					int superstepNo = controlMsg.getSuperstepNo();

					// Log query superstep stats
					if (EnableQueryStats && superstepNo >= 0 && queryStatsSteps != null && msgActiveQuery != null
							&& msgQueryOnWorker != null) {
						List<Q> step = queryStatsSteps.get(msgQueryOnWorker.QueryId);
						if (step != null) {
							step.add(msgActiveQuery.QueryStepAggregator);
							List<Long> statsTimes = queryStatsStepTimes.get(msgQueryOnWorker.QueryId);
							if (statsTimes != null) {
								statsTimes.add((System.nanoTime() - msgActiveQuery.LastStepTime));
							}
						}
					}

					boolean queryFinished;
					if (msgActiveQuery.QueryStepAggregator.masterForceAllWorkersActive(superstepNo)) {
						msgActiveQuery.ActiveWorkers.addAll(workerIds);
					}
					if (msgActiveQuery.ActiveWorkers.isEmpty()) {
						queryFinished = msgActiveQuery.QueryStepAggregator.onMasterAllVerticesFinished();
						if (!queryFinished) msgActiveQuery.ActiveWorkers.addAll(workerIds);
					}
					else {
						queryFinished = false;
					}

					// All workers have superstep finished
					if (!queryFinished) {
						// Active workers, start next superstep
						msgActiveQuery.LastFinishedSuperstepNo++;
						queryNextSuperstepReady(msgActiveQuery, msgActiveQuery.StartedSuperstepNo);
					}
					else {
						// All workers finished, finish query
						msgActiveQuery.workersFinished(workerIds);
						signalWorkersQueryFinish(msgActiveQuery.BaseQuery);
						logger.debug("All workers no more active for query " + msgActiveQuery.BaseQuery.QueryId + ":"
								+ msgActiveQuery.StartedSuperstepNo + " after "
								+ (System.nanoTime() - msgActiveQuery.StartTime) / 1000000 + "ms");

						// Log query total stats
						if (EnableQueryStats) {
							queryStatsTotals.put(msgQueryOnWorker.QueryId, msgActiveQuery.QueryTotalAggregator);
						}
					}
				}
			}
			else { // Worker_Query_Finished
				if (msgActiveQuery.IsComputing) {
					logger.error("Query " + msgQueryOnWorker.QueryId + " still computing, wrong message: "
							+ msgQueryOnWorker);
					return;
				}

				msgActiveQuery.workersWaitingFor.remove(srcMachine);
				logger.debug("Worker {} finished query {}", new Object[] { srcMachine, msgActiveQuery.BaseQuery.QueryId });

				if (msgActiveQuery.workersWaitingFor.isEmpty()) {
					// All workers have query finished
					logger.info("All workers finished query " + msgActiveQuery.BaseQuery.QueryId);
					evaluateQueryResult(msgActiveQuery);
					synchronized (activeQueries) {
						activeQueries.remove(msgActiveQuery.BaseQuery.QueryId);
					}
					long duration = System.nanoTime() - msgActiveQuery.StartTime;
					queryStartTimes.put(msgActiveQuery.BaseQuery.QueryId, msgActiveQuery.StartTime);
					queryDurations.put(msgActiveQuery.BaseQuery.QueryId, duration);
					logger.info("# Evaluated finished query " + msgActiveQuery.BaseQuery.QueryId + " after "
							+ (duration / 1000000) + "ms, " + msgActiveQuery.StartedSuperstepNo + " steps, "
							+ msgActiveQuery.QueryTotalAggregator.toString());

					// Start next query batch if ready for this now
					if (queriesReadyForNextStep.size() >= activeQueries.size()) startReadyQueriesSupersteps();
				}
			}
		}
		else if (controlMsg.getType() == ControlMessageType.Worker_Barrier_Finished) {
			if (globalBarrierWaitSet.contains(controlMsg.getSrcMachine())) {
				logger.info("Worker finished global barrier " + controlMsg.getSrcMachine());
				globalBarrierWaitSet.remove(controlMsg.getSrcMachine());
				if (globalBarrierWaitSet.isEmpty()) {
					globalBarrierFinished();
				}
			}
			else {
				logger.error("Worker_Barrier_Finished from worker not waiting for " + controlMsg.getSrcMachine() + " " + controlMsg);
			}
		}
		else if (controlMsg.getType() == ControlMessageType.Worker_Query_Vertex_Chunks) {
			QueryVertexChunksMessage vertexChunks = controlMsg.getQueryVertexChunks();
			Map<IntSet, Integer> queryChunks = new HashMap<>();
			for (QueryVertexChunksMapMessage chunk : vertexChunks.getChunksList()) {
				queryChunks.put(new IntOpenHashSet(chunk.getQueriesList()), chunk.getCount());
			}
			int workerId = controlMsg.getSrcMachine();
			vertexMoveDeciderService.updateQueryIntersects(workerId, queryChunks,
					MiscUtil.defaultLong(latestWorkerActiveVerticesWindows.get(workerId))); // TODO Ok?
			//MiscUtil.defaultLong(latestWorkerTotalVertices2.get(workerId)));
		}
		else {
			logger.error("Unexpected control message type in message " + message);
		}
	}


	private void initializeWorkersAssignPartitions() {
		Map<Integer, List<String>> assignedPartitions;
		try {
			logger.debug("Start input partitioning/assigning");
			assignedPartitions = inputPartitioner.partition(inputFile, inputPartitionDir, workerIds);
			logger.debug("Finished input partitioning");
		}
		catch (IOException e) {
			logger.error("Error at partitioning", e);
			return;
		}
		for (final Integer workerId : workerIds) {
			List<String> partitions = assignedPartitions.get(workerId);
			logger.debug("Assign partitions to {}: {}", new Object[] { workerId, partitions });
			messaging.sendControlMessageUnicast(workerId,
					ControlMessageBuildUtil.Build_Master_WorkerInitialize(ownId, partitions, masterStartTimeMs), true);
		}
		logger.debug("Start input assigning");
	}

	private void handleStartQuery(Q query) {
		FileUtil.createDirOrEmptyFiles(outputDir + File.separator + Integer.toString(query.QueryId));
		query.setVertexCount(vertexCount);
		MasterQuery<Q> activeQuery = new MasterQuery<>(query, workerIds, queryValueFactory);
		synchronized (activeQueries) {
			activeQueries.put(query.QueryId, activeQuery);
			queuedQueries--;
		}

		Map<Integer, Integer> queryWorkerAVerts = new HashMap<>();
		for (Integer worker : workerIds) {
			queryWorkerAVerts.put(worker, 0);
		}

		// Start query on workers
		signalWorkersQueryStart(query);
		logger.info("Master started query " + query.QueryId);
	}

	private void signalWorkersQueryStart(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryStart(ownId, query), true);
	}

	/**
	 * Let workers start next superstep of a query. Make decisions about vertices to move.
	 */
	private void queryNextSuperstepReady(MasterQuery<Q> queryReady, int superstepNo) {

		queriesReadyForNextStep.add(queryReady);
		//logger.info("#+# " + queryReady.BaseQuery.QueryId + ":" + queryReady.StartedSuperstepNo + " " + queriesReadyForNextStep);

		// Only start queries in a batch. Avoids superstep chaos and allows global barriers.
		if (queryGlobalBarrier && queriesReadyForNextStep.size() < activeQueries.size()) return;

		//logger.info("#+-------------------------------------------------");

		if (vertexMoveDeciderService.hasNewDecission() && VertexMoveEnabled && activeQueries.size() == Configuration.MAX_PARALLEL_QUERIES) {
			VertexMoveDecision newMoveDecission = vertexMoveDeciderService.getNewDecission();

			double delta = localSuperstepsRatioUnique - lastQMoveLSRU;
			boolean lsruThreshold = localSuperstepsRatioUnique < LsruThresholdLow
					|| delta < LsruDeltaThresholdNeg || delta > LsruDeltaThresholdPos;
			boolean imbalanceThreshold = latestWavwMaxImbalance > (VertexMaxActVertsImbalance + VertexMaxActVertsImbTrigger)
					|| latestWavwAvgImbalance > (VertexAvgActVertsImbalance + VertexAvgActVertsImbTrigger); // TODO Config

			if (lsruThreshold || imbalanceThreshold || remainingLsruExtraShots > 0) {
				System.err.println("Start QMove: " + localSuperstepsRatioUnique + " last " + lastQMoveLSRU + " delta " + delta
						+ " avgImbalance " + latestWavwAvgImbalance + " maxImbalance " + latestWavwMaxImbalance + " "
						+ latestWorkerActiveVerticesWindows + " " + lsruThreshold + " " + imbalanceThreshold);//TODO Testcode
				logger.info("Start QMove: " + localSuperstepsRatioUnique + " last " + lastQMoveLSRU + " delta " + delta
						+ " avgImbalance " + latestWavwAvgImbalance + " maxImbalance " + latestWavwMaxImbalance + " "
						+ latestWorkerActiveVerticesWindows);

				// Allow extra shot of one move wasnt successful
				if (lsruThreshold) {
					lastQMoveLSRU = localSuperstepsRatioUnique;
					remainingLsruExtraShots = LsruExtraShots;
				}
				else if (!imbalanceThreshold) {
					remainingLsruExtraShots--;
					System.err.println("Used LsruExtraShot, remaining " + remainingLsruExtraShots);//TODO Log
					logger.info("Used LsruExtraShot, remaining " + remainingLsruExtraShots);//TODO Log
				}


				if (!globalBarrierPlanned) {
					if (newMoveDecission != null) {
						// New move decision
						moveDecission = newMoveDecission;
						globalBarrierPlanned = true;
						logger.info("New move decission, plan barrier");
					}
				}
				else if (newMoveDecission != null) {
					// Replace previous move decision
					moveDecission = newMoveDecission;
					logger.info("Replace move decission, barrier already planned");
				}
			}
			else {
				System.err.println("Suspend QMove: " + localSuperstepsRatioUnique + " last " + lastQMoveLSRU + " delta " + delta
						+ " avgImbalance " + latestWavwAvgImbalance + " maxImbalance " + latestWavwMaxImbalance + " "
						+ latestWorkerActiveVerticesWindows);
				logger.info("Suspend QMove: " + localSuperstepsRatioUnique + " last " + lastQMoveLSRU + " delta " + delta
						+ " avgImbalance " + latestWavwAvgImbalance + " maxImbalance " + latestWavwMaxImbalance + " "
						+ latestWorkerActiveVerticesWindows);//TODO Only log debug
			}
		}

		if (globalBarrierPlanned) {
			if (queriesReadyForNextStep.size() < activeQueries.size()) return;

			// Start global barrier and move vertices
			globalBarrierPlanned = false;
			if (moveDecission == null) {
				logger.error("Global barrier planned but no moveDecission");
				globalBarrierFinished();
				return;
			}

			logger.info("Starting barrier for move");
			globalBarrierWaitSet.addAll(workerIds);

			// Map of query finished supersteps for this barrier
			Map<Integer, Integer> queryFinishedSupersteps = new HashMap<>(activeQueries.size());
			for (MasterQuery<Q> q : activeQueries.values()) {
				// Activate all workers for following superstep - vertices might be moved
				q.ActiveWorkers.addAll(workerIds);
				queryFinishedSupersteps.put(q.BaseQuery.QueryId, q.StartedSuperstepNo);
			}

			// Send barrier move messages
			for (int workerId : workerIds) {
				messaging.sendControlMessageUnicast(workerId, ControlMessageBuildUtil.Build_Master_StartBarrier_VertexMove(ownId,
						moveDecission.WorkerVertSendMsgs.get(workerId), moveDecission.WorkerVertRecvMsgs.get(workerId),
						queryFinishedSupersteps), true);
			}
			logger.info("Started barrier with vertex move");
			logger.debug("Supersteps at vertex move: {}", queryFinishedSupersteps);
			moveDecission = null;
		}
		else {
			// Normal start next superstep of queries
			startReadyQueriesSupersteps();
		}
	}


	private void startQueryNextSuperstep(MasterQuery<Q> queryToStart) {
		queryToStart.beginStartNextSuperstep(workerIds);

		Set<Integer> queryActiveWorkers;
		if (skipInactiveWorkers)
			queryActiveWorkers = new HashSet<>(queryToStart.ActiveWorkers);
		else queryActiveWorkers = new HashSet<>(workerIds);

		logger.trace("Next superstep {}:{} with {}/{} {}", new Object[] { queryToStart.BaseQuery.QueryId,
				queryToStart.StartedSuperstepNo, queryActiveWorkers.size(), workerIds.size(), queryActiveWorkers });

		if (queryActiveWorkers.size() == 1 && localQueryExecution) {
			logger.trace("localmode on {} {}:{}", new Object[] { queryActiveWorkers, queryToStart.BaseQuery.QueryId,
					queryToStart.StartedSuperstepNo });
			queryToStart.IsInLocalMode = true;

			// Start query in localmode - only one worker runs query until it is finished or not local anymore
			for (Integer workerId : workerIds) {
				if (queryActiveWorkers.contains(workerId)) {
					// This worker is the chosen one - it can execute the query in localmode
					messaging.sendControlMessageUnicast(workerId,
							ControlMessageBuildUtil.Build_Master_QueryNextSuperstep(queryToStart.StartedSuperstepNo,
									ownId, queryToStart.QueryStepAggregator, WorkerQueryExecutionMode.LocalOnThis,
									new ArrayList<>(0)),
							true);
				}
				else {
					// This worker will not execute the query until it leaves localmode
					messaging.sendControlMessageUnicast(workerId,
							ControlMessageBuildUtil.Build_Master_QueryNextSuperstep(queryToStart.StartedSuperstepNo,
									ownId, queryToStart.QueryStepAggregator, WorkerQueryExecutionMode.LocalOnOther,
									new ArrayList<>(queryActiveWorkers)),
							true);
				}
			}
			finishStartNextSuperstep(queryToStart);
		}
		else {
			// Start query superstep in normal mode
			queryToStart.IsInLocalMode = false;

			for (Integer workerId : workerIds) {
				List<Integer> otherActiveWorkers = new ArrayList<>(queryActiveWorkers.size());
				for (Integer activeWorkerId : queryActiveWorkers) {
					if (!workerId.equals(activeWorkerId)) otherActiveWorkers.add(activeWorkerId);
				}

				WorkerQueryExecutionMode skipMode = (skipInactiveWorkers && !queryActiveWorkers.contains(workerId))
						? WorkerQueryExecutionMode.NonLocalSkip : WorkerQueryExecutionMode.NonLocal;
				messaging.sendControlMessageUnicast(workerId,
						ControlMessageBuildUtil.Build_Master_QueryNextSuperstep(queryToStart.StartedSuperstepNo, ownId,
								queryToStart.QueryStepAggregator, skipMode,
								otherActiveWorkers),
						true);
			}
			finishStartNextSuperstep(queryToStart);
		}
	}

	private void finishStartNextSuperstep(MasterQuery<Q> queryToStart) {
		logger.debug("Workers finished superstep, now starting " + queryToStart.BaseQuery.QueryId + ":"
				+ (queryToStart.StartedSuperstepNo) + " after "
				+ ((System.nanoTime() - queryToStart.LastStepTime) / 1000000) + "ms. Total "
				+ ((System.nanoTime() - queryToStart.StartTime) / 1000000) + "ms. Active: "
				+ queryToStart.QueryStepAggregator.getActiveVertices());
		queryToStart.finishStartNextSuperstep();
		logger.trace("Next master superstep query {}:{}", new Object[] { queryToStart.BaseQuery.QueryId, queryToStart.StartedSuperstepNo });
	}

	private void globalBarrierFinished() {
		logger.info("Global barrier finished");
		startReadyQueriesSupersteps();
	}

	/**
	 * Starts all quries ready in readyQueryNextSteps.
	 */
	private void startReadyQueriesSupersteps() {
		logger.debug("Start query supersteps batch: {}", queriesReadyForNextStep);
		//logger.info("/////// Start query supersteps batch: {}", queriesReadyForNextStep);

		for (MasterQuery<Q> delayedQueryNextStep : queriesReadyForNextStep) {
			logger.debug("Start query superstep {}", delayedQueryNextStep.BaseQuery.QueryId);
			startQueryNextSuperstep(delayedQueryNextStep);
		}
		for (Integer workerId : workerIds) {
			messaging.flushAsyncChannel(workerId);
		}
		queriesReadyForNextStep.clear();
	}



	private void signalWorkersQueryFinish(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryFinish(ownId, query), true);
	}

	private void signalWorkersShutdown() {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_Shutdown(ownId), true);
	}



	// #################### Stop, save and print #################### //
	@Override
	public void stop() {
		finishedTimeSinceStart = (System.currentTimeMillis() - masterStartTimeMs);
		finishedTimeSinceFirstQuery = (System.currentTimeMillis() - firstQueryStartTimeMs);
		logger.info("Stopping master after " + finishedTimeSinceStart + "ms");
		logger.info("--- Time since first query " + finishedTimeSinceFirstQuery + "ms ---");

		signalWorkersShutdown();
		vertexMoveDeciderService.stop();
		super.stop();
		saveWorkerStats();
		saveQueryStats();

		// Silly waiting, somethimes files not finished
		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		plotStats();
		printErrorCount();
	}

	private void printErrorCount() {
		ErrWarnCounter.Enabled = false;
		if (ErrWarnCounter.Warnings > 0)
			logger.warn("Warnings: " + ErrWarnCounter.Warnings);
		else logger.info("No warnings");
		if (ErrWarnCounter.Errors > 0)
			logger.error("Errors: " + ErrWarnCounter.Errors);
		else logger.info("No errors");
	}

	private void saveWorkerStats() {
		StringBuilder sb = new StringBuilder();
		List<String> workerStatsNames = new ArrayList<>(new WorkerStats().getStatsMap(workerIds.size()).keySet());
		Collections.sort(workerStatsNames);

		// Worker times in milliseconds, normalized for time/s
		for (Integer workerId : workerIds) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "worker" + workerId + "_times_ms.csv"))) {
				writer.println(
						"SumTime;ComputeTime;IdleTime;QueryWaitTime;StepFinishTime;IntersectCalcTime;MoveSendVerticesTime;MoveRecvVerticesTime;HandleMessagesTime;BarrierStartWaitTime;BarrierFinishWaitTime;BarrierVertexMoveTime;");

				for (Pair<Long, WorkerStats> statSample : workerStats.get(workerId)) {
					double timeNormFactor = 1;
					Map<String, Double> statsMap = statSample.second.getStatsMap(workerIds.size());

					double sumTime = statsMap.get("ComputeTime") + statsMap.get("StepFinishTime") + statsMap.get("IntersectCalcTime")
							+ statsMap.get("IdleTime") + statsMap.get("QueryWaitTime")
							+ statsMap.get("MoveSendVerticesTime") + statsMap.get("MoveRecvVerticesTime")
							+ statsMap.get("HandleMessagesTime") + statsMap.get("BarrierStartWaitTime")
							+ statsMap.get("BarrierFinishWaitTime") + statsMap.get("BarrierVertexMoveTime");
					sb.append(sumTime / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("ComputeTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("IdleTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("QueryWaitTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("StepFinishTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("IntersectCalcTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("MoveSendVerticesTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("MoveRecvVerticesTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("HandleMessagesTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("BarrierStartWaitTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("BarrierFinishWaitTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("BarrierVertexMoveTime") / 1000000 * timeNormFactor);
					sb.append(';');

					writer.println(sb.toString());
					sb.setLength(0);
				}
			}
			catch (Exception e) {
				logger.error("Exception when saveQueryStats", e);
			}
		}

		// Worker times in milliseconds, normalized for time/s
		for (Integer workerId : workerIds) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "worker" + workerId + "_times_normed_ms.csv"))) {
				writer.println(
						"SumTime;ComputeTime;IdleTime;QueryWaitTime;StepFinishTime;IntersectCalcTime;MoveSendVerticesTime;MoveRecvVerticesTime;HandleMessagesTime;BarrierStartWaitTime;BarrierFinishWaitTime;BarrierVertexMoveTime;");

				long lastTime = 0;
				for (Pair<Long, WorkerStats> statSample : workerStats.get(workerId)) {
					long timeSinceLastSample = statSample.first - lastTime;
					lastTime = statSample.first;
					double timeNormFactor = (double) 1000 / timeSinceLastSample;
					Map<String, Double> statsMap = statSample.second.getStatsMap(workerIds.size());

					double sumTime = statsMap.get("ComputeTime") + statsMap.get("StepFinishTime") + statsMap.get("IntersectCalcTime")
							+ statsMap.get("IdleTime") + statsMap.get("QueryWaitTime")
							+ statsMap.get("MoveSendVerticesTime") + statsMap.get("MoveRecvVerticesTime")
							+ statsMap.get("HandleMessagesTime") + statsMap.get("BarrierStartWaitTime")
							+ statsMap.get("BarrierFinishWaitTime") + statsMap.get("BarrierVertexMoveTime");
					sb.append(sumTime / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("ComputeTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("IdleTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("QueryWaitTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("StepFinishTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("IntersectCalcTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("MoveSendVerticesTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("MoveRecvVerticesTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("HandleMessagesTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("BarrierStartWaitTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("BarrierFinishWaitTime") / 1000000 * timeNormFactor);
					sb.append(';');
					sb.append(statsMap.get("BarrierVertexMoveTime") / 1000000 * timeNormFactor);
					sb.append(';');

					writer.println(sb.toString());
					sb.setLength(0);
				}
			}
			catch (Exception e) {
				logger.error("Exception when saveQueryStats", e);
			}
		}

		// Worker individual stats
		Map<String, Double> workerStatsSums = new HashMap<>();
		for (int workerId : workerIds) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "worker" + workerId + "_all.csv"))) {
				// Header line
				sb.append("Time(s);");
				for (String statName : workerStatsNames) {
					sb.append(statName);
					sb.append(';');
				}
				writer.println(sb.toString());
				sb.setLength(0);

				// Values
				List<Pair<Long, WorkerStats>> statSamples = workerStats.get(workerId);
				for (int iSample = 1; iSample < statSamples.size(); iSample++) {
					Pair<Long, WorkerStats> statSample = statSamples.get(iSample);
					sb.append((double) statSample.first / 1000); // Timestamp in seconds
					sb.append(';');
					Map<String, Double> sampleValues = statSample.second.getStatsMap(workerIds.size());
					for (String statName : workerStatsNames) {
						double statValue = sampleValues.get(statName);
						workerStatsSums.put(statName,
								MiscUtil.defaultDouble(workerStatsSums.get(statName)) + statValue);
						sb.append(statValue);
						sb.append(';');
					}
					writer.println(sb.toString());
					sb.setLength(0);
				}
			}
			catch (Exception e) {
				logger.error("Exception when saveWorkerStats", e);
			}
		}


		// Worker average stats
		try (PrintWriter writer = new PrintWriter(
				new FileWriter(queryStatsDir + File.separator + "workerAvgStats.csv"))) {
			// Header line
			sb.append("Time;");
			for (String statName : workerStatsNames) {
				sb.append(statName);
				sb.append(';');
			}
			writer.println(sb.toString());
			sb.setLength(0);

			Map<String, Double> statSums = new HashMap<>();
			breakAvgStatsLoop: for (int i = 1; true; i++) {
				long avgTime = 0;
				statSums.clear();
				for (int workerId : workerIds) {
					List<Pair<Long, WorkerStats>> workerStatSamples = workerStats.get(workerId);
					if (workerStatSamples.size() <= i) {
						break breakAvgStatsLoop;
					}
					Pair<Long, WorkerStats> statSample = workerStatSamples.get(i);
					avgTime += statSample.first;

					for (Entry<String, Double> stat : statSample.second.getStatsMap(workerIds.size()).entrySet()) {
						MiscUtil.mapAdd(statSums, stat.getKey(), stat.getValue());
					}
				}

				avgTime /= workerIds.size();
				sb.append((double) avgTime / 1000);
				sb.append(';');

				for (String statName : workerStatsNames) {
					sb.append(statSums.get(statName) / workerIds.size());
					sb.append(';');
				}
				writer.println(sb.toString());
				sb.setLength(0);
			}
		}
		catch (Exception e) {
			logger.error("Exception when saveWorkerStats", e);
		}



		double totalSupersteps = workerStatsSums.get("SuperstepsComputed");
		double localSupersteps = workerStatsSums.get("LocalSuperstepsComputed");
		double nonlocalSuperstepsUnique = (totalSupersteps - localSupersteps) / workerIds.size();
		double totalSuperstepsUnique = localSupersteps + nonlocalSuperstepsUnique;
		workerStatsSums.put("LocalSuperstepsRatio", localSupersteps * 100 / totalSupersteps);
		workerStatsSums.put("SuperstepsComputedUnique", totalSuperstepsUnique);
		workerStatsSums.put("LocalSuperstepsRatioUnique", localSupersteps * 100 / totalSuperstepsUnique);
		List<String> workerStatsSumsNames = new ArrayList<>(workerStatsSums.keySet());
		Collections.sort(workerStatsSumsNames);
		try (PrintWriter writer = new PrintWriter(
				new FileWriter(queryStatsDir + File.separator + "allworkers" + "_all.csv"))) {
			for (String statName : workerStatsSumsNames) {
				writer.println(statName + ";" + workerStatsSums.get(statName) + ";");
			}
		}
		catch (Exception e) {
			logger.error("Exception when saveWorkerStats", e);
		}
		logger.info("Saved worker stats");

		try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "summary.txt"))) {
			writer.println("TimeSinceStart: " + finishedTimeSinceStart);
			writer.println("TimeSinceFirstQuery: " + finishedTimeSinceFirstQuery);
			writer.println("LocalSuperstepsRatio: " + workerStatsSums.get("LocalSuperstepsRatio"));
			writer.println("LocalSuperstepsRatioUnique: " + workerStatsSums.get("LocalSuperstepsRatioUnique"));
			writer.println("SuperstepsComputed: " + workerStatsSums.get("SuperstepsComputed"));
			writer.println("SuperstepsComputedUnique: " + workerStatsSums.get("SuperstepsComputedUnique"));
		}
		catch (Exception e) {
			logger.error("Exception when saveWorkerStats", e);
		}
		logger.info("Saved summary");

		try (PrintWriter writer = new PrintWriter(new FileWriter(queryStatsDir + File.separator + "workerStatsSums.txt"))) {
			List<String> keys = new ArrayList<>(workerStatsSums.keySet());
			Collections.sort(keys);
			for (String key : keys) {
				writer.println(key + ": " + workerStatsSums.get(key));
			}
		}
		catch (Exception e) {
			logger.error("Exception when workerStatsSums", e);
		}
		logger.info("Saved workerStatsSums");
	}

	private void saveQueryStats() {
		if (!EnableQueryStats) return;
		StringBuilder sb = new StringBuilder();

		// Query times in milliseconds. Step time is how long a step took, worker time is the time workers spent calculating.
		for (Entry<Integer, List<Q>> querySteps : queryStatsSteps.entrySet()) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "query" + querySteps.getKey() + "_times_ms.csv"))) {
				writer.println(
						"StepTime;WorkerTime;ComputeTime;StepFinishTime;");

				for (int i = 0; i < querySteps.getValue().size(); i++) {
					Q step = querySteps.getValue().get(i);
					if (step != null) {
						List<Long> st = queryStatsStepTimes.get(querySteps.getKey());
						if (st != null) {
							Long stepTime = st.get(i);
							if (stepTime != null) {
								sb.append(stepTime / 1000000);
							}
						}
						sb.append(';');

						sb.append(step.Stats.getTimeSum() / 1000000);
						sb.append(';');
						sb.append(step.Stats.ComputeTime / 1000000);
						sb.append(';');
						sb.append(step.Stats.StepFinishTime / 1000000);
						sb.append(';');
					}
					writer.println(sb.toString());
					sb.setLength(0);
				}
			}
			catch (Exception e) {
				logger.error("Exception when saveQueryStats", e);
			}
		}


		// All stats for all queries on workers
		for (Entry<Integer, List<SortedMap<Integer, Q>>> querySteps : queryStatsStepMachines.entrySet()) {
			for (Integer workerId : workerIds) {
				try (PrintWriter writer = new PrintWriter(
						new FileWriter(
								queryStatsDir + File.separator + "query" + querySteps.getKey() + "_worker" + workerId + "_all.csv"))) {
					// Write first line
					sb.append("ActiveVertices");
					sb.append(';');
					for (String colName : new QueryStats().getStatsMap(workerIds.size()).keySet()) {
						sb.append(colName);
						sb.append(';');
					}
					writer.println(sb.toString());
					sb.setLength(0);

					for (int i = 0; i < querySteps.getValue().size(); i++) {
						Q step = querySteps.getValue().get(i).get(workerId);
						if (step == null) continue; // Missing steps when doing localmode

						sb.append(step.getActiveVertices());
						sb.append(';');

						for (Double colStat : step.Stats.getStatsMap(workerIds.size()).values()) {
							sb.append(colStat);
							sb.append(';');
						}
						writer.println(sb.toString());
						sb.setLength(0);
					}
				}
				catch (Exception e) {
					logger.error("Exception when saveQueryStats", e);
				}
			}
		}


		// Query summary
		try (PrintWriter writer = new PrintWriter(
				new FileWriter(queryStatsDir + File.separator + "queries.csv"))) {
			writer.println(
					"QueryId;QueryHash;QueryStartTime;QueryFinishTime;QueryDuration;Duration (ms);WorkerTime (ms);ComputeTime (ms);");
			for (Entry<Integer, Q> query : queryStatsTotals.entrySet()) {
				long qStartTime = (queryStartTimes.get(query.getKey()) - firstQueryStartTimeNano) / 1000000;
				long qDuration = queryDurations.get(query.getKey()) / 1000000;
				writer.println(
						query.getKey() + ";"
								+ query.getValue().GetQueryHash() + ";"
								+ qStartTime + ";"
								+ (qStartTime + qDuration) + ";"
								+ qDuration + ";"
								+ query.getValue().Stats.getTimeSum() / 1000000 + ";"
								+ query.getValue().Stats.getTimeSum() / 1000000 + ";"
								+ query.getValue().Stats.ComputeTime / 1000000 + ";");
			}
		}
		catch (Exception e) {
			logger.error("Exception when saveQueryStats", e);
		}
		try (PrintWriter writer = new PrintWriter(new FileWriter(queryStatsDir + File.separator + "queries_all.csv"))) {
			List<String> statNames = null;
			for (Q query : queryStatsTotals.values()) {
				statNames = new ArrayList<>(query.Stats.getStatsMap(workerIds.size()).keySet());
				break;
			}
			if (statNames != null) {
				Collections.sort(statNames);
				writer.println(String.join(";", statNames));

				for (Entry<Integer, Q> query : queryStatsTotals.entrySet()) {
					sb.setLength(0);
					Map<String, Double> qStats = query.getValue().Stats.getStatsMap(workerIds.size());

					for (String stat : statNames) {
						sb.append(qStats.get(stat));
						sb.append(';');
					}
					writer.println(sb.toString());
				}
			}
		}
		catch (Exception e) {
			logger.error("Exception when saveQueryStats", e);
		}

		logger.info("Saved query stats");
	}

	private void plotStats() {

		// Plotting
		if (Configuration.getPropertyBoolDefault("PlotWorkerStats", false)
				|| Configuration.getPropertyBoolDefault("PlotQueryStats", false)) {
			try {
				JFreeChartPlotter.plotStats(outputDir, 1);
				JFreeChartPlotter.plotStats(outputDir, 4);
			}
			catch (Exception e) {
				logger.error("Exception when plot stats", e);
			}
		}
		logger.info("Plotted stats");
	}

	private void saveConfigSummary() {
		// Copy configuration
		try {
			Files.copy(new File(Configuration.ConfigFile).toPath(),
					new File(outputDir + File.separator + "configuration.properties").toPath());
		}
		catch (Exception e) {
			logger.error("Exception when copy config", e);
		}
	}

	private void saveSetupSummary(Map<Integer, MachineConfig> machines, int ownId) {

		// Setup summary
		try (PrintWriter writer = new PrintWriter(
				new FileWriter(outputDir + File.separator + "setup.txt"))) {
			writer.println("MasterID: " + ownId);
			writer.println("Machines: ");
			for (Entry<Integer, MachineConfig> machine : machines.entrySet()) {
				writer.println("\t" + machine.getKey() + "\t" + machine.getValue().HostName + ":" + machine.getValue().MessagePort);
			}
		}
		catch (Exception e) {
			logger.error("Exception when save setup stats", e);
		}
	}


	private void evaluateQueryResult(MasterQuery<Q> query) {
		// Aggregate output
		try {
			outputCombiner.evaluateOutput(outputDir + File.separator + query.BaseQuery.QueryId, query.QueryTotalAggregator);
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}
	}
}
