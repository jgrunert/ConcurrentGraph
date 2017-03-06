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
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;
import mthesis.concurrent_graph.communication.ProtoEnvelopeMessage;
import mthesis.concurrent_graph.logging.ErrWarnCounter;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.plotting.JFreeChartPlotter;
import mthesis.concurrent_graph.util.FileUtil;
import mthesis.concurrent_graph.util.MiscUtil;
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

	private long masterStartTime;
	private final List<Integer> workerIds;
	private final Set<Integer> workersToInitialize;
	private Map<Integer, MasterQuery<Q>> activeQueries = new HashMap<>();

	/** Map<Machine, Map<QueryId, ActiveVertexCount>> */
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

	private final String inputFile;
	private final String inputPartitionDir;
	private final String outputDir;
	private int vertexCount;
	private final MasterInputPartitioner inputPartitioner;
	private final MasterOutputEvaluator<Q> outputCombiner;
	private final BaseQueryGlobalValuesFactory<Q> queryValueFactory;


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
	}

	@Override
	public void start() {
		masterStartTime = System.nanoTime();
		super.start();
		// Initialize workers
		initializeWorkersAssignPartitions(); // Signal workers to initialize
		logger.info("Workers partitions assigned and initialize starting after "
				+ ((System.nanoTime() - masterStartTime) / 1000000) + "ms");
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
						+ " vertices after " + ((System.nanoTime() - masterStartTime) / 1000000) + "ms");
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
					// Wrong message count should match broadcast message count, therwise there might be communication errors.
					if (workerIds.size() > 1
							&& msgActiveQuery.QueryStepAggregator.Stats.MessagesReceivedWrongVertex != msgActiveQuery.QueryStepAggregator.Stats.MessagesSentBroadcast
									/ (workerIds.size() - 1) * (workerIds.size() - 2)) {
						// TODO Investigate why happening
						//						logger.warn(String.format(
						//								"Unexpected wrong vertex message count %d does not match broadcast message count %d. Should be %d. Possible communication errors.",
						//								msgActiveQuery.QueryStepAggregator.Stats.MessagesReceivedWrongVertex,
						//								msgActiveQuery.QueryStepAggregator.Stats.MessagesSentBroadcast,
						//								msgActiveQuery.QueryStepAggregator.Stats.MessagesSentBroadcast / (workerIds.size() - 1) *
						//										(workerIds.size() - 2)));
					}

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
					logger.info("# Evaluated finished query " + msgActiveQuery.BaseQuery.QueryId + " after "
							+ ((System.nanoTime() - msgActiveQuery.StartTime) / 1000000) + "ms, "
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
		saveQueryStats();
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

	private void saveQueryStats() {
		if (!enableQueryStats) return;
		StringBuilder sb = new StringBuilder();

		// Query active vertices
		for (Entry<Integer, List<SortedMap<Integer, Q>>> querySteps : queryStatsStepMachines.entrySet()) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + querySteps.getKey() + "_query_actverts.csv"))) {
				for (Integer workerId : workerIds) {
					sb.append("Worker " + workerId);
					sb.append(';');
				}
				writer.println(sb.toString());
				sb.setLength(0);

				for (Map<Integer, Q> step : querySteps.getValue()) {
					for (Q stepMachine : step.values()) {
						sb.append(stepMachine.getActiveVertices());
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


		// Query times in milliseconds
		for (Entry<Integer, List<Q>> querySteps : queryStatsSteps.entrySet()) {
			try (PrintWriter writer = new PrintWriter(
					new FileWriter(queryStatsDir + File.separator + querySteps.getKey() + "_times_ms.csv"))) {
				writer.println("StepTime;TotalTimes;ComputeTime;StepFinishTime;IntersectCalcTime;MoveSendVerticsTime;MoveRecvVerticsTime;");

				for (int i = 0; i < querySteps.getValue().size(); i++) {
					Q step = querySteps.getValue().get(i);
					long compTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.ComputeTimeKey));
					long intersCalcTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.IntersectCalcTimeKey));
					long stepFinishTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.StepFinishTimeKey));
					long moveSendTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.MoveSendVerticsTimeKey));
					long moveRecvTime = MiscUtil.defaultLong(step.Stats.OtherStats.get(QueryStats.MoveRecvVerticsTimeKey));

					sb.append(queryStatsStepTimes.get(querySteps.getKey()).get(i) / 1000000);
					sb.append(';');
					sb.append((compTime + intersCalcTime + stepFinishTime + moveSendTime + moveRecvTime) / 1000000);
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
			writer.println("Query;QueryHash;ComputeTime (ms);");
			for (Entry<Integer, Q> query : queryStatsTotals.entrySet()) {
				writer.println(
						query.getKey() + ";" + query.getValue().GetQueryHash() + ";"
								+ query.getValue().Stats.getStatValue(("ComputeTime")) / 1000000 + ";");
			}
		}
		catch (Exception e) {
			logger.error("Exception when saveQueryStats", e);
		}


		// Plotting
		if (Configuration.getPropertyBool("OutputPlots")) {
			try {
				JFreeChartPlotter.plotStats(queryStatsDir);
			}
			catch (Exception e) {
				logger.error("Exception when plot stats", e);
			}
		}
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
				writer.println("\t" + machine.getKey() + " " + machine.getValue().HostName + ":" + machine.getValue().MessagePort);
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
					ControlMessageBuildUtil.Build_Master_WorkerInitialize(ownId, assignedPartitions.get(workerId)),
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
	private void startWorkersQueryNextSuperstep(Q query, int superstepNo) {

		//		System.out.println("/////");

		if (Configuration.VERTEX_MOVE_ENABLED) {
			// Evaluate intersections, decide if move vertices
			Map<Integer, Integer> workerActiveVerts = actQueryWorkerActiveVerts.get(query.QueryId);
			Map<Integer, Integer> workerIntersectsSum = new HashMap<>(workerActiveVerts.size());
			for (Entry<Integer, Map<Integer, Integer>> wIntersects : actQueryWorkerIntersects.get(query.QueryId).entrySet()) {
				int intersectSum = 0;
				for (Integer intersect : wIntersects.getValue().values()) {
					intersectSum += intersect;
				}
				workerIntersectsSum.put(wIntersects.getKey(), intersectSum);
				// TODO Testcode
				//				if (intersectSum > 0) {
				//					System.err.println("INTERSECT " + wIntersects.getKey() + " " + wIntersects);
				//				}
			}

			// TODO Just a simple test algorithm: Move all other vertices to worker with most active vertices
			List<Integer> sortedWorkers = new ArrayList<>(MiscUtil.sortByValueInverse(workerActiveVerts).keySet());
			int receivingWorker = sortedWorkers.get(0);
			List<Integer> sendingWorkers = new ArrayList<>(sortedWorkers.size() - 1);
			List<Integer> notSendingWorkers = new ArrayList<>(sortedWorkers.size() - 1);
			for (int i = 1; i < sortedWorkers.size(); i++) {
				int workerId = sortedWorkers.get(i);
				// Only move vertices from workers with active, query-exclusive vertices
				if (workerActiveVerts.get(workerId) > 0 && workerIntersectsSum.get(workerId) == 0)
					sendingWorkers.add(workerId);
				else
					notSendingWorkers.add(workerId);
			}

			//			System.out.println(workerActiveVerts);

			// TODO Testcode
			//			if (!sendingWorkers.isEmpty())
			//				System.out.println(query.QueryId + ":" + superstepNo + " receivingWorker " + receivingWorker
			//						+ " sendingWorkers " + sendingWorkers + " notSendingWorkers " + notSendingWorkers);

			messaging.sendControlMessageUnicast(receivingWorker,
					ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_VertReceive(superstepNo, ownId, query, sendingWorkers), true);
			for (Integer otherWorkerId : sendingWorkers) {
				messaging.sendControlMessageUnicast(otherWorkerId,
						ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_VertSend(superstepNo, ownId, query, receivingWorker), true);
			}
			for (Integer otherWorkerId : notSendingWorkers) {
				messaging.sendControlMessageUnicast(otherWorkerId,
						ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_NoVertMove(superstepNo, ownId, query), true);
			}
		}
		else {
			// No vertex move
			for (Integer otherWorkerId : workerIds) {
				messaging.sendControlMessageUnicast(otherWorkerId,
						ControlMessageBuildUtil.Build_Master_QueryNextSuperstep_NoVertMove(superstepNo, ownId, query), true);
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
