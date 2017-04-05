package mthesis.concurrent_graph.master;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.logging.ErrWarnCounter;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.master.vertexmove.AbstractVertexMoveDecider;
import mthesis.concurrent_graph.master.vertexmove.GreedyCostBasedVertexMoveDecider;
import mthesis.concurrent_graph.master.vertexmove.VertexMoveDecision;
import mthesis.concurrent_graph.plotting.JFreeChartPlotter;
import mthesis.concurrent_graph.util.FileUtil;
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

	private long masterStartTimeMs;
	private long masterStartTimeNano;
	private final List<Integer> workerIds;
	private final Set<Integer> workersToInitialize;
	private Map<Integer, MasterQuery<Q>> activeQueries = new HashMap<>();

	/** Map<QueryId, Map<MachineId, ActiveVertexCount>> */
	private Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts = new HashMap<>();
	/** Map<Machine, Map<QueryId, Map<IntersectWithWorkerId, IntersectingsCount>>> */
	private Map<Integer, Map<Integer, Map<Integer, Integer>>> actQueryWorkerIntersects = new HashMap<>();

	// Query logging for later evaluation
	private final boolean enableQueryStats = true;
	private final String queryStatsDir;
	private final Map<Integer, List<SortedMap<Integer, Q>>> queryStatsStepMachines = new HashMap<>();
	private final Map<Integer, List<Q>> queryStatsSteps = new HashMap<>();
	private final Map<Integer, List<Long>> queryStatsStepTimes = new HashMap<>();
	private final Map<Integer, Q> queryStatsTotals = new HashMap<>();
	private final Map<Integer, Long> queryDurations = new HashMap<>();

	// Map WorkerId->(timestamp, workerStatsSample)
	private Map<Integer, List<Pair<Long, WorkerStats>>> workerStats = new HashMap<>();

	private final String inputFile;
	private final String inputPartitionDir;
	private final String outputDir;
	private int vertexCount;
	private final MasterInputPartitioner inputPartitioner;
	private final MasterOutputEvaluator<Q> outputCombiner;
	private final BaseQueryGlobalValuesFactory<Q> queryValueFactory;

	private final AbstractVertexMoveDecider<Q> vertexMoveDecider = new GreedyCostBasedVertexMoveDecider<>();


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
		FileUtil.makeCleanDirectory(outputDir);
		FileUtil.makeCleanDirectory(queryStatsDir);
		saveSetupSummary(machines, ownId);
		saveConfigSummary();

		for (Integer workerId : workerIds) {
			workerStats.put(workerId, new ArrayList<>());
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
	}


	/**
	 * Starts a new query on this master and its workers
	 */
	public void startQuery(Q query) {
		// Get query ready
		if (activeQueries.containsKey(query.QueryId))
			throw new RuntimeException("There is already an active query with this ID: " + query.QueryId);

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

		if (enableQueryStats) {
			queryStatsStepMachines.put(query.QueryId, new ArrayList<>());
			queryStatsSteps.put(query.QueryId, new ArrayList<>());
			queryStatsStepTimes.put(query.QueryId, new ArrayList<>());
		}

		if (activeQueries.size() >= Configuration.MAX_PARALLEL_QUERIES) {
			logger.info("Wait for activeQueries<MAX_PARALLEL_QUERIES " + Configuration.MAX_PARALLEL_QUERIES
					+ " before starting query: " + query.QueryId);
			while (activeQueries.size() >= Configuration.MAX_PARALLEL_QUERIES) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		synchronized (this) {
			FileUtil.makeCleanDirectory(outputDir + File.separator + Integer.toString(query.QueryId));
			query.setVertexCount(vertexCount);
			MasterQuery<Q> activeQuery = new MasterQuery<>(query, workerIds, queryValueFactory);
			activeQueries.put(query.QueryId, activeQuery);

			Map<Integer, Integer> queryWorkerAVerts = new HashMap<>();
			for (Integer worker : workerIds) {
				queryWorkerAVerts.put(worker, 0);
			}
			actQueryWorkerActiveVerts.put(query.QueryId, queryWorkerAVerts);
			actQueryWorkerIntersects.put(query.QueryId, new HashMap<>());
		}

		// Start query on workers
		signalWorkersQueryStart(query);
		logger.info("Master started query " + query.QueryId);
	}

	public boolean isQueryActive(int queryId) {
		return activeQueries.containsKey(queryId);
	}

	public void waitForQueryFinish(int queryId) {
		while (activeQueries.containsKey(queryId)) {
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
		while (!activeQueries.isEmpty()) {
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
	public synchronized void onIncomingMessage(ChannelMessage message) {
		// TODO No more super.onIncomingControlMessage(message);

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
			logger.debug("Worker initialized: " + srcMachine);

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
			if (controlMsg.getSuperstepNo() != msgActiveQuery.SuperstepNo) {
				logger.error("Message for wrong superstep. not " + msgActiveQuery.SuperstepNo + ": " + message);
				return;
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
				for (WorkerStatSample sample : samples) {
					workerStats.get(controlMsg.getSrcMachine())
							.add(new Pair<Long, WorkerStats>(sample.getTime(), new WorkerStats(sample.getStatsBytes())));
				}
			}

			// Query intersects on machine
			Map<Integer, Integer> queryIntersects = controlMsg.getQueryIntersections().getIntersectionsMap();
			actQueryWorkerIntersects.get(msgQueryOnWorker.QueryId).put(srcMachine, queryIntersects);

			if (controlMsg.getType() == ControlMessageType.Worker_Query_Superstep_Finished) {
				if (!msgActiveQuery.IsComputing) {
					logger.error(
							"Query " + msgQueryOnWorker.QueryId + " not computing, wrong message: " + msgQueryOnWorker);
					return;
				}

				actQueryWorkerActiveVerts.get(msgQueryOnWorker.QueryId).put(srcMachine,
						msgQueryOnWorker.getActiveVertices());
				msgActiveQuery.aggregateQuery(msgQueryOnWorker);

				// Log worker superstep stats
				if (enableQueryStats && controlMsg.getSuperstepNo() >= 0) {
					List<SortedMap<Integer, Q>> queryStepList = queryStatsStepMachines.get(msgQueryOnWorker.QueryId);
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

					// Log query superstep stats
					if (enableQueryStats && controlMsg.getSuperstepNo() >= 0) {
						queryStatsSteps.get(msgQueryOnWorker.QueryId).add(msgActiveQuery.QueryStepAggregator);
						queryStatsStepTimes.get(msgQueryOnWorker.QueryId).add((System.nanoTime() - msgActiveQuery.LastStepTime));
					}

					// All workers have superstep finished
					if (msgActiveQuery.ActiveWorkers > 0 || controlMsg.getSuperstepNo() < 0) {
						// Active workers, start next superstep
						msgActiveQuery.nextSuperstep(workerIds);
						startWorkersQueryNextSuperstep(msgActiveQuery.QueryStepAggregator, msgActiveQuery.SuperstepNo);
						msgActiveQuery.resetValueAggregator(queryValueFactory);
						msgActiveQuery.LastStepTime = System.nanoTime();
						logger.debug("Workers finished superstep " + msgActiveQuery.BaseQuery.QueryId + ":"
								+ (msgActiveQuery.SuperstepNo - 1) + " after "
								+ ((System.nanoTime() - msgActiveQuery.LastStepTime) / 1000000) + "ms. Total "
								+ ((System.nanoTime() - msgActiveQuery.StartTime) / 1000000) + "ms. Active: "
								+ msgActiveQuery.QueryStepAggregator.getActiveVertices());
						logger.trace("Next master superstep query " + msgActiveQuery.BaseQuery.QueryId + ": "
								+ msgActiveQuery.SuperstepNo);

						// TODO Testcode
						// Time: 6623ms/5900ms
						//						System.out.println(msgActiveQuery.SuperstepNo);
					}
					else {
						// All workers finished, finish query
						msgActiveQuery.workersFinished(workerIds);
						signalWorkersQueryFinish(msgActiveQuery.BaseQuery);
						logger.info("All workers no more active for query " + msgActiveQuery.BaseQuery.QueryId + ":"
								+ msgActiveQuery.SuperstepNo + " after "
								+ (System.nanoTime() - msgActiveQuery.StartTime) / 1000000 + "ms");

						// Log query total stats
						if (enableQueryStats) {
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
				logger.info("Worker " + srcMachine + " finished query " + msgActiveQuery.BaseQuery.QueryId);

				if (msgActiveQuery.workersWaitingFor.isEmpty()) {
					// All workers have query finished
					logger.info("All workers finished query " + msgActiveQuery.BaseQuery.QueryId);
					evaluateQueryResult(msgActiveQuery);
					activeQueries.remove(msgActiveQuery.BaseQuery.QueryId);
					actQueryWorkerActiveVerts.remove(msgActiveQuery.BaseQuery.QueryId);
					long duration = System.nanoTime() - msgActiveQuery.StartTime;
					queryDurations.put(msgActiveQuery.BaseQuery.QueryId, duration);
					logger.info("# Evaluated finished query " + msgActiveQuery.BaseQuery.QueryId + " after "
							+ (duration / 1000000) + "ms, " + msgActiveQuery.SuperstepNo + " steps, "
							+ msgActiveQuery.QueryTotalAggregator.toString());
				}
			}
		}
		else {
			logger.error("Unexpected control message type in message " + message);
		}
	}

	@Override
	public void stop() {
		signalWorkersShutdown();
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
		if (ErrWarnCounter.Errors > 0)
			logger.warn("Errors: " + ErrWarnCounter.Errors);
		if (ErrWarnCounter.Warnings == 0 && ErrWarnCounter.Errors == 0)
			logger.info("No warnings or errors");
	}

	private void saveWorkerStats() {
		StringBuilder sb = new StringBuilder();
		Set<String> statsNames = new WorkerStats().getStatsMap().keySet();

		// Worker times in milliseconds, normalized for time/s
		for (Integer workerId : workerIds) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "worker" + workerId + "_times_ms.csv"))) {
				writer.println(
						"SumTime;ComputeTime;IdleTime;QueryWaitTime;StepFinishTime;IntersectCalcTime;MoveSendVerticesTime;MoveRecvVerticesTime;HandleMessagesTime;BarrierStartWaitTime;BarrierFinishWaitTime;BarrierVertexMoveTime;");

				long lastTime = 0;
				for (Pair<Long, WorkerStats> statSample : workerStats.get(workerId)) {
					long timeSinceLastSample = statSample.first - lastTime;
					lastTime = statSample.first;
					double timeNormFactor = (double) 1000 / timeSinceLastSample;
					Map<String, Double> statsMap = statSample.second.getStatsMap();

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

		// Worker all stats
		for (int workerId : workerIds) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "worker" + workerId + "_all.csv"))) {
				// Header line
				sb.append("Time(s);");
				for (String statName : statsNames) {
					sb.append(statName);
					sb.append(';');
				}
				writer.println(sb.toString());
				sb.setLength(0);

				// Values
				for (Pair<Long, WorkerStats> statSample : workerStats.get(workerId)) {
					sb.append(statSample.first / 1000); // Timestamp in seconds
					sb.append(';');
					Map<String, Double> sampleValues = statSample.second.getStatsMap();
					for (String statName : statsNames) {
						sb.append(sampleValues.get(statName));
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
		logger.info("Saved worker stats");
	}

	private void saveQueryStats() {
		if (!enableQueryStats) return;
		StringBuilder sb = new StringBuilder();

		// Query times in milliseconds. Step time is how long a step took, worker time is the time workers spent calculating.
		for (Entry<Integer, List<Q>> querySteps : queryStatsSteps.entrySet()) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + "query" + querySteps.getKey() + "_times_ms.csv"))) {
				writer.println(
						"StepTime;WorkerTime;ComputeTime;StepFinishTime;IntersectCalcTime;MoveSendVerticsTime;MoveRecvVerticsTime;");

				for (int i = 0; i < querySteps.getValue().size(); i++) {
					Q step = querySteps.getValue().get(i);
					sb.append(queryStatsStepTimes.get(querySteps.getKey()).get(i) / 1000000);
					sb.append(';');
					sb.append(step.Stats.getWorkersTime() / 1000000);
					sb.append(';');
					sb.append(step.Stats.ComputeTime / 1000000);
					sb.append(';');
					sb.append(step.Stats.IntersectCalcTime / 1000000);
					sb.append(';');
					sb.append(step.Stats.StepFinishTime / 1000000);
					sb.append(';');
					sb.append(step.Stats.MoveSendVerticesTime / 1000000);
					sb.append(';');
					sb.append(step.Stats.MoveRecvVerticesTime / 1000000);
					sb.append(';');
					writer.println(sb.toString());
					sb.setLength(0);
				}
			}
			catch (Exception e) {
				logger.error("Exception when saveQueryStats", e);
			}
		}


		// All stats for all workers
		for (Entry<Integer, List<SortedMap<Integer, Q>>> querySteps : queryStatsStepMachines.entrySet()) {
			for (Integer workerId : workerIds) {
				try (PrintWriter writer = new PrintWriter(
						new FileWriter(
								queryStatsDir + File.separator + "query" + querySteps.getKey() + "_worker" + workerId + "_all.csv"))) {
					// Write first line
					sb.append("ActiveVertices");
					sb.append(';');
					for (String colName : new QueryStats().getStatsMap().keySet()) {
						sb.append(colName);
						sb.append(';');
					}
					writer.println(sb.toString());
					sb.setLength(0);

					for (int i = 0; i < querySteps.getValue().size(); i++) {
						Q step = querySteps.getValue().get(i).get(workerId);

						sb.append(step.getActiveVertices());
						sb.append(';');

						for (Double colStat : step.Stats.getStatsMap().values()) {
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
			writer.println("Query;QueryHash;Duration (ms);WorkerTime (ms);ComputeTime (ms);");
			for (Entry<Integer, Q> query : queryStatsTotals.entrySet()) {
				writer.println(
						query.getKey() + ";" + query.getValue().GetQueryHash() + ";"
								+ queryDurations.get(query.getKey()) / 1000000 + ";"
								+ query.getValue().Stats.getWorkersTime() / 1000000 + ";"
								+ query.getValue().Stats.ComputeTime / 1000000 + ";");
			}
		}
		catch (Exception e) {
			logger.error("Exception when saveQueryStats", e);
		}

		logger.info("Saved worker stats");
	}

	private void plotStats() {

		// Plotting
		if (Configuration.getPropertyBool("OutputPlots")) {
			try {
				JFreeChartPlotter.plotStats(outputDir);
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



	@Override
	public void run() {
		// TODO Remove
	}


	private void evaluateQueryResult(MasterQuery<Q> query) {
		// Aggregate output
		try {
			outputCombiner.evaluateOutput(outputDir + File.separator + query.BaseQuery.QueryId, query.BaseQuery);
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}
	}


	private void initializeWorkersAssignPartitions() {
		Map<Integer, List<String>> assignedPartitions;
		try {
			logger.debug("Start input partitioning/assigning");
			assignedPartitions = inputPartitioner.partition(inputFile, inputPartitionDir,
					workerIds);
			logger.debug("Finished input partitioning");
		}
		catch (IOException e) {
			logger.error("Error at partitioning", e);
			return;
		}
		for (final Integer workerId : workerIds) {
			List<String> partitions = assignedPartitions.get(workerId);
			logger.debug("Assign partitions to " + workerId + ": " + partitions);
			messaging.sendControlMessageUnicast(workerId,
					ControlMessageBuildUtil.Build_Master_WorkerInitialize(ownId, partitions, masterStartTimeMs),
					true);
		}
		logger.debug("Start input assigning");
	}

	private void signalWorkersQueryStart(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryStart(ownId, query),
				true);
		//		for (Integer workerId : workerIds) {
		//			messaging.sendControlMessageUnicast(workerId, ControlMessageBuildUtil.Build_Master_QueryStart(ownId, query),
		//					true);
		//			try {
		//				Thread.sleep(1000);
		//			}
		//			catch (InterruptedException e) {
		//				// TODO Auto-generated catch block
		//				e.printStackTrace();
		//			}
		//		}
	}

	/**
	 * Let workers start next superstep of a query. Make decisions about vertices to move.
	 */
	private void startWorkersQueryNextSuperstep(Q queryToStart, int superstepNo) {

		long decideStartTime = System.currentTimeMillis();
		VertexMoveDecision moveDecission = vertexMoveDecider.decide(workerIds, activeQueries, actQueryWorkerActiveVerts,
				actQueryWorkerIntersects);

		if (moveDecission != null) {
			System.out.println("Decided in " + (System.currentTimeMillis() - decideStartTime)); // TODO Master stats

			// Send barrier move messages
			for (int workerId : workerIds) {
				messaging.sendControlMessageUnicast(workerId,
						ControlMessageBuildUtil.Build_Master_StartBarrier_VertexMove(ownId,
								moveDecission.WorkerVertSendMsgs.get(workerId), moveDecission.WorkerVertRecvMsgs.get(workerId)),
						true);
			}
			logger.info("Starting barrier with vertex move");
			// TODO Wait for barrier finish?
		}
		// No vertex move
		for (Integer otherWorkerId : workerIds) {
			messaging.sendControlMessageUnicast(otherWorkerId,
					ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_NoVertMove(superstepNo, ownId, queryToStart), true);
		}
	}



	private void signalWorkersQueryFinish(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryFinish(ownId, query),
				true);
	}

	private void signalWorkersShutdown() {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_Shutdown(ownId),
				true);
	}
}
