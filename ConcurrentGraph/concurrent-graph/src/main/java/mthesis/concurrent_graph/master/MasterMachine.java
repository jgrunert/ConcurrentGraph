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
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.logging.ErrWarnCounter;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
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
public class MasterMachine<Q extends BaseQueryGlobalValues> extends AbstractMachine<NullWritable, NullWritable, NullWritable, Q> {

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

	private long vertexBarrierMoveLastTime = 0;


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
		while (!workersToInitialize.isEmpty()) {
			logger.info("Wait for workersInitialized before starting query: " + query.QueryId);
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		if (enableQueryStats) {
			queryStatsStepMachines.put(query.QueryId, new ArrayList<>());
			queryStatsSteps.put(query.QueryId, new ArrayList<>());
			queryStatsStepTimes.put(query.QueryId, new ArrayList<>());
		}

		while (activeQueries.size() >= Configuration.MAX_PARALLEL_QUERIES) {
			logger.info("Wait for activeQueries<MAX_PARALLEL_QUERIES before starting query: " + query.QueryId);
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
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
				return;
			}
		}
	}


	@Override
	public synchronized void onIncomingMessage(ChannelMessage message) {
		// TODO No more super.onIncomingControlMessage(message);

		if (message.getTypeCode() != 1) {
			logger.error("Master machine can only handle ProtoEnvelopeMessage");
			return;
		}
		MessageEnvelope protoMsg = ((ProtoEnvelopeMessage) message).message;
		if (!protoMsg.hasControlMessage()) {
			logger.error("Master machine can only handle ProtoEnvelopeMessage with ControlMessage");
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
				if (enableQueryStats) {
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
					if (enableQueryStats) {
						queryStatsSteps.get(msgQueryOnWorker.QueryId).add(msgActiveQuery.QueryStepAggregator);
						queryStatsStepTimes.get(msgQueryOnWorker.QueryId).add((System.nanoTime() - msgActiveQuery.LastStepTime));
					}

					// All workers have superstep finished
					if (msgActiveQuery.ActiveWorkers > 0) {
						// Active workers, start next superstep
						msgActiveQuery.nextSuperstep(workerIds);
						startWorkersQueryNextSuperstep(msgActiveQuery.QueryStepAggregator, msgActiveQuery.SuperstepNo);
						msgActiveQuery.resetValueAggregator(queryValueFactory);
						msgActiveQuery.LastStepTime = System.nanoTime();
						logger.debug("Workers finished superstep " + msgActiveQuery.BaseQuery.QueryId + ":"
								+ msgActiveQuery.SuperstepNo + " after "
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
							+ (duration / 1000000) + "ms, "
							+ msgActiveQuery.QueryTotalAggregator.Stats.getOtherStatsString());
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
		for (int workerId : workerIds) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + workerId + "_workerstats.csv"))) {
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
					Map<String, Long> sampleValues = statSample.second.getStatsMap();
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

		// Query active vertices
		//		for (Entry<Integer, List<SortedMap<Integer, Q>>> querySteps : queryStatsStepMachines.entrySet()) {
		//			try (PrintWriter writer = new PrintWriter(
		//					new FileWriter(queryStatsDir + File.separator + querySteps.getKey() + "_query_actverts.csv"))) {
		//				for (Integer workerId : workerIds) {
		//					sb.append("Worker " + workerId);
		//					sb.append(';');
		//				}
		//				writer.println(sb.toString());
		//				sb.setLength(0);
		//
		//				for (Map<Integer, Q> step : querySteps.getValue()) {
		//					for (Q stepMachine : step.values()) {
		//						sb.append(stepMachine.getActiveVertices());
		//						sb.append(';');
		//					}
		//					writer.println(sb.toString());
		//					sb.setLength(0);
		//				}
		//			}
		//			catch (Exception e) {
		//				logger.error("Exception when saveQueryStats", e);
		//			}
		//		}


		// Query times in milliseconds. Step time is how long a step took, worker time is the time workers spent calculating.
		for (Entry<Integer, List<Q>> querySteps : queryStatsSteps.entrySet()) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + querySteps.getKey() + "_times_ms.csv"))) {
				writer.println(
						"StepTime;WorkerTime;ComputeTime;StepFinishTime;IntersectCalcTime;MoveSendVerticsTime;MoveRecvVerticsTime;");

				for (int i = 0; i < querySteps.getValue().size(); i++) {
					Q step = querySteps.getValue().get(i);
					long workersTime = step.Stats.getWorkersTime();
					long compTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.ComputeTimeKey));
					long intersCalcTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.IntersectCalcTimeKey));
					long stepFinishTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.StepFinishTimeKey));
					long moveSendTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.MoveSendVerticsTimeKey));
					long moveRecvTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.MoveRecvVerticsTimeKey));

					sb.append(queryStatsStepTimes.get(querySteps.getKey()).get(i) / 1000000);
					sb.append(';');
					sb.append(workersTime / 1000000);
					sb.append(';');
					sb.append(compTime / 1000000);
					sb.append(';');
					sb.append(intersCalcTime / 1000000);
					sb.append(';');
					sb.append(stepFinishTime / 1000000);
					sb.append(';');
					sb.append(moveSendTime / 1000000);
					sb.append(';');
					sb.append(moveRecvTime / 1000000);
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
						new FileWriter(queryStatsDir + File.separator + querySteps.getKey() + "_" + workerId + "_all.csv"))) {
					// Write first line
					sb.append("ActiveVertices");
					sb.append(';');
					for (String colName : QueryStats.AllStatsNames) {
						sb.append(colName);
						sb.append(';');
					}
					writer.println(sb.toString());
					sb.setLength(0);

					for (int i = 0; i < querySteps.getValue().size(); i++) {
						Q step = querySteps.getValue().get(i).get(workerId);

						sb.append(step.getActiveVertices());
						sb.append(';');

						for (String colName : QueryStats.AllStatsNames) {
							sb.append(step.Stats.getStatValue(colName));
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
								+ query.getValue().Stats.getStatValue(("ComputeTime")) / 1000000 + ";");
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
			Files.copy(new File(Configuration.CONFIG_FILE).toPath(),
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
			assignedPartitions = inputPartitioner.partition(inputFile, inputPartitionDir,
					workerIds);
		}
		catch (IOException e) {
			logger.error("Error at partitioning", e);
			return;
		}
		for (final Integer workerId : workerIds) {
			messaging.sendControlMessageUnicast(workerId,
					ControlMessageBuildUtil.Build_Master_WorkerInitialize(ownId, assignedPartitions.get(workerId), masterStartTimeMs),
					true);
		}
	}

	private void signalWorkersQueryStart(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryStart(ownId, query),
				true);
	}

	/**
	 * Let workers start next superstep of a query. Make decisions about vertices to move.
	 */
	private void startWorkersQueryNextSuperstep(Q queryToStart, int superstepNo) {

		// Total worker load
		// TODO Diagram output
		Map<Integer, Integer> workersActiveVerts = new HashMap<>(actQueryWorkerActiveVerts.size());
		for (Entry<Integer, Map<Integer, Integer>> queryWorkers : actQueryWorkerActiveVerts.entrySet()) {
			for (Entry<Integer, Integer> workerActVerts : queryWorkers.getValue().entrySet()) {
				Integer workerVerts = MiscUtil.defaultInt(workersActiveVerts.get(workerActVerts.getKey()));
				workersActiveVerts.put(workerActVerts.getKey(), workerVerts + workerActVerts.getValue());
			}
		}

		double tolerableAvVariance = 0.2;

		int workerActVertAvg = 0;
		for (Integer workerActVerts : workersActiveVerts.values()) {
			workerActVertAvg += workerActVerts;
		}
		workerActVertAvg /= workersActiveVerts.size();

		if (Configuration.VERTEX_BARRIER_MOVE_ENABLED && (System.currentTimeMillis()
				- vertexBarrierMoveLastTime) > Configuration.VERTEX_BARRIER_MOVE_INTERVAL) {
			boolean anyMoves = false;
			Map<Integer, List<Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage>> workerVertSendMsgs = new HashMap<>();
			Map<Integer, List<Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage>> workerVertRecvMsgs = new HashMap<>();
			for (int workerId : workerIds) {
				workerVertSendMsgs.put(workerId, new ArrayList<>());
				workerVertRecvMsgs.put(workerId, new ArrayList<>());
			}

			// Calculate query moves
			for (MasterQuery<Q> activeQuery : activeQueries.values()) {
				Map<Integer, Integer> queriesWorkerActiveVerts = actQueryWorkerActiveVerts.get(activeQuery.BaseQuery.QueryId);
				Map<Integer, Integer> queriesWorkerIntersectsSum = new HashMap<>(queriesWorkerActiveVerts.size());
				for (Entry<Integer, Map<Integer, Integer>> wIntersects : actQueryWorkerIntersects.get(activeQuery.BaseQuery.QueryId)
						.entrySet()) {
					int intersectSum = 0;
					for (Integer intersect : wIntersects.getValue().values()) {
						intersectSum += intersect;
					}
					queriesWorkerIntersectsSum.put(wIntersects.getKey(), intersectSum);
					// TODO Testcode
					//				if (intersectSum > 0) {
					//					System.err.println("INTERSECT " + wIntersects.getKey() + " " + wIntersects);
					//				}
				}

				// TODO Just a simple test algorithm:
				// Move all other vertices to worker with most active vertices and sub-average active vertices
				List<Integer> sortedWorkers = new ArrayList<>(MiscUtil.sortByValueInverse(queriesWorkerActiveVerts).keySet());
				int recvWorkerIndexTmp = 0;
				while (workersActiveVerts.get(sortedWorkers.get(recvWorkerIndexTmp)) > workerActVertAvg) {
					recvWorkerIndexTmp++;
				}
				int receivingWorker = sortedWorkers.get(recvWorkerIndexTmp);
				sortedWorkers.remove(recvWorkerIndexTmp);

				for (int i = 0; i < sortedWorkers.size(); i++) {
					int workerId = sortedWorkers.get(i);
					double workerVaDiff = workerActVertAvg > 0
							? (double) (workersActiveVerts.get(workerId) - workerActVertAvg) / workerActVertAvg
							: 0;
					// Only move vertices from workers with active, query-exclusive vertices and enough active vertices
					if (queriesWorkerActiveVerts.get(workerId) > 0 && queriesWorkerIntersectsSum.get(workerId) == 0 &&
							workerVaDiff > -tolerableAvVariance) {
						// TODO Modify queriesWorkerActiveVerts according to movement
						anyMoves = true;
						workerVertSendMsgs.get(workerId).add(
								Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage.newBuilder()
										.setQueryId(activeQuery.BaseQuery.QueryId)
										.setMoveToMachine(receivingWorker).setMaxMoveCount(Integer.MAX_VALUE)
										.build());
						workerVertRecvMsgs.get(receivingWorker).add(
								Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage.newBuilder()
										.setQueryId(activeQuery.BaseQuery.QueryId)
										.setReceiveFromMachine(workerId).build());
					}
				}
			}

			// Send barrier move messages
			if (anyMoves) {
				for (int workerId : workerIds) {
					messaging.sendControlMessageUnicast(workerId,
							ControlMessageBuildUtil.Build_Master_StartBarrier_VertexMove(ownId,
									workerVertSendMsgs.get(workerId), workerVertRecvMsgs.get(workerId)),
							true);
				}
				logger.info("Starting barrier with vertex move");
			}
			else {
				logger.info("No vertices to move, no barrier");
			}
			vertexBarrierMoveLastTime = System.currentTimeMillis();
		}

		//		if (Configuration.VERTEX_LIVE_MOVE_ENABLED) {
		//			// Evaluate intersections, decide if move vertices
		//			Map<Integer, Integer> queriesWorkerActiveVerts = actQueryWorkerActiveVerts.get(query.QueryId);
		//			Map<Integer, Integer> queriesWorkerIntersectsSum = new HashMap<>(queriesWorkerActiveVerts.size());
		//			for (Entry<Integer, Map<Integer, Integer>> wIntersects : actQueryWorkerIntersects.get(query.QueryId).entrySet()) {
		//				int intersectSum = 0;
		//				for (Integer intersect : wIntersects.getValue().values()) {
		//					intersectSum += intersect;
		//				}
		//				queriesWorkerIntersectsSum.put(wIntersects.getKey(), intersectSum);
		//				// TODO Testcode
		//				//				if (intersectSum > 0) {
		//				//					System.err.println("INTERSECT " + wIntersects.getKey() + " " + wIntersects);
		//				//				}
		//			}
		//
		//			// TODO Just a simple test algorithm:
		//			// Move all other vertices to worker with most active vertices and sub-average active vertices
		//			List<Integer> sortedWorkers = new ArrayList<>(MiscUtil.sortByValueInverse(queriesWorkerActiveVerts).keySet());
		//			int recvWorkerIndex = 0;
		//			//			while (workersActiveVerts.get(sortedWorkers.get(recvWorkerIndex)) > workerActVertAvg) {
		//			//				recvWorkerIndex++;
		//			//			}
		//			int receivingWorker = sortedWorkers.get(recvWorkerIndex);
		//			sortedWorkers.remove(recvWorkerIndex);
		//
		//			List<Integer> sendingWorkers = new ArrayList<>(sortedWorkers.size() - 1);
		//			List<Integer> notSendingWorkers = new ArrayList<>(sortedWorkers.size() - 1);
		//			for (int i = 0; i < sortedWorkers.size(); i++) {
		//				int workerId = sortedWorkers.get(i);
		//				double workerVaDiff = workerActVertAvg > 0
		//						? (double) (workersActiveVerts.get(workerId) - workerActVertAvg) / workerActVertAvg
		//						: 0;
		//				// Only move vertices from workers with active, query-exclusive vertices and enough active vertices
		//				if (queriesWorkerActiveVerts.get(workerId) > 0 && queriesWorkerIntersectsSum.get(workerId) == 0 &&
		//						workerVaDiff > -tolerableAvVariance)
		//					sendingWorkers.add(workerId);
		//				else
		//					notSendingWorkers.add(workerId);
		//			}
		//
		//			//			else
		//			//				System.out.println("" + workersActiveVerts + " " + workerActVertAvg);
		//
		//
		//			//			System.out.println(workerActiveVerts);
		//
		//			// TODO Testcode
		//			//			if (!sendingWorkers.isEmpty())
		//			//				System.out.println(query.QueryId + ":" + superstepNo + " receivingWorker " + receivingWorker
		//			//						+ " sendingWorkers " + sendingWorkers + " notSendingWorkers " + notSendingWorkers);
		//
		//
		//			if (!sendingWorkers.isEmpty()) {
		//				//			if (notSendingWorkers.isEmpty()) {
		//				System.out.println(
		//						"YES !!!!!!! " + workersActiveVerts + " " + workerActVertAvg + " " + sendingWorkers + " " + notSendingWorkers);
		//
		//				// Vertex sending
		//				messaging.sendControlMessageUnicast(receivingWorker,
		//						ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_VertReceive(superstepNo, ownId, query, sendingWorkers),
		//						true);
		//				for (Integer otherWorkerId : sendingWorkers) {
		//					messaging.sendControlMessageUnicast(otherWorkerId,
		//							ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_VertSend(superstepNo, ownId, query, receivingWorker),
		//							true);
		//				}
		//				for (Integer otherWorkerId : notSendingWorkers) {
		//					messaging.sendControlMessageUnicast(otherWorkerId,
		//							ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_NoVertMove(superstepNo, ownId, query), true);
		//				}
		//			}
		//			else {
		//				// No vertex senders
		//				for (Integer otherWorkerId : workerIds) {
		//					messaging.sendControlMessageUnicast(otherWorkerId,
		//							ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_NoVertMove(superstepNo, ownId, query), true);
		//				}
		//			}
		//		}
		//		else
		{
			// No vertex move
			for (Integer otherWorkerId : workerIds) {
				messaging.sendControlMessageUnicast(otherWorkerId,
						ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_NoVertMove(superstepNo, ownId, queryToStart), true);
			}
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
