package mthesis.concurrent_graph.master;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mthesis.concurrent_graph.AbstractMachine;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.util.FileUtil;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Concurrent graph processing master main
 *
 * @author Jonas Grunert
 *
 * @param <Q>
 *            Global query values
 */
public class MasterMachine<Q extends BaseQueryGlobalValues> extends AbstractMachine<NullWritable> {

	private long masterStartTime;
	private final List<Integer> workerIds;
	private final Set<Integer> workersToInitialize;
	private Map<Integer, ActiveQuery<Q>> activeQueries = new HashMap<>();

	private final String inputFile;
	private final String inputPartitionDir;
	private final String outputDir;
	private int vertexCount;
	private final MasterInputPartitioner inputPartitioner;
	private final MasterOutputEvaluator<Q> outputCombiner;
	private final BaseQueryGlobalValuesFactory<Q> queryValueFactory;


	public MasterMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, String inputFile,
			String inputPartitionDir, MasterInputPartitioner inputPartitioner, MasterOutputEvaluator<Q> outputCombiner, String outputDir,
			BaseQueryGlobalValuesFactory<Q> globalValueFactory) {
		super(machines, ownId, null);
		this.workerIds = workerIds;
		this.workersToInitialize = new HashSet<>(workerIds);
		this.vertexCount = 0;
		this.inputFile = inputFile;
		this.inputPartitionDir = inputPartitionDir;
		this.inputPartitioner = inputPartitioner;
		this.outputCombiner = outputCombiner;
		this.outputDir = outputDir;
		this.queryValueFactory = globalValueFactory;
		FileUtil.makeCleanDirectory(outputDir);
	}

	@Override
	public void start() {
		masterStartTime = System.currentTimeMillis();
		super.start();
		// Initialize workers
		initializeWorkersAssignPartitions(); // Signal workers to initialize
		logger.info("Workers partitins assigned and initialize starting after " + (System.currentTimeMillis() - masterStartTime) + "ms");
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
		FileUtil.makeCleanDirectory(outputDir + File.separator + Integer.toString(query.QueryId));
		query.setVertexCount(vertexCount);
		ActiveQuery<Q> activeQuery = new ActiveQuery<>(query, workerIds, queryValueFactory);
		activeQueries.put(query.QueryId, activeQuery);

		// Start query on workers
		signalWorkersQueryStart(query);
		logger.info("Master started query " + query.QueryId);
	}


	@Override
	public synchronized void onIncomingControlMessage(ControlMessage message) {
		// TODO No more super.onIncomingControlMessage(message);

		// Process query
		if (message.getType() == ControlMessageType.Worker_Initialized) {
			// Process Worker_Initialized message
			if (workersToInitialize.isEmpty()) {
				logger.error("Received Worker_Initialized but all workers intitialized");
				return;
			}

			workersToInitialize.remove(message.getSrcMachine());
			vertexCount += message.getWorkerInitialized().getVertexCount();
			logger.debug("Worker initialized: " + message.getSrcMachine());

			if (workersToInitialize.isEmpty()) {
				logger.info("All workers initialized, " + workerIds.size() + " workers, " + vertexCount + " vertices after " +
						(System.currentTimeMillis() - masterStartTime) + "ms");
			}
		}
		else if (message.getType() == ControlMessageType.Worker_Query_Superstep_Finished
				|| message.getType() == ControlMessageType.Worker_Query_Finished) {
			// Process worker query message
			if (!workersToInitialize.isEmpty()) {
				logger.error("Received non-Worker_Initialized but not all workers intitialized");
				return;
			}

			// Get message query
			if (message.getQueryValues().size() == 0) throw new RuntimeException("Control message without query: " + message);
			Q msgQueryOnWorker = queryValueFactory.createFromBytes(ByteBuffer.wrap(message.getQueryValues().toByteArray()));
			ActiveQuery<Q> msgActiveQuery = activeQueries.get(msgQueryOnWorker.QueryId);
			if (msgActiveQuery == null) throw new RuntimeException("Control message without ungknown query: " + message);


			if (message.getSuperstepNo() != msgActiveQuery.SuperstepNo) {
				logger.error("Message for wrong superstep. not " + msgActiveQuery.SuperstepNo + ": " + message);
				return;
			}

			if (!msgActiveQuery.workersWaitingFor.contains(message.getSrcMachine())) {
				logger.error("Query " + msgQueryOnWorker.QueryId + " not waiting for " + msgQueryOnWorker);
				return;
			}
			msgActiveQuery.workersWaitingFor.remove(message.getSrcMachine());

			// TODO re-introduce QuerySuperstepStats?
			//			int SentControlMessages = 0;
			//			int SentVertexMessagesLocal = 0;
			//			int SentVertexMessagesUnicast = 0;
			//			int SentVertexMessagesBroadcast = 0;
			//			int SentVertexMessagesBuckets = 0;
			//			int ReceivedCorrectVertexMessages = 0;
			//			int ReceivedWrongVertexMessages = 0;
			//			int newVertexMachinesDiscovered = 0;
			//			int totalVertexMachinesDiscovered = 0;

			if (message.getType() == ControlMessageType.Worker_Query_Superstep_Finished) {
				if (!msgActiveQuery.IsComputing) {
					logger.error("Query " + msgQueryOnWorker.QueryId + " not computing, wrong message: " + msgQueryOnWorker);
					return;
				}

				msgActiveQuery.aggregateQuery(msgQueryOnWorker);

				if (msgActiveQuery.workersWaitingFor.isEmpty()) {
					// All workers have superstep finished
					if (msgActiveQuery.ActiveWorkers > 0) {
						// Active workers, start next superstep
						msgActiveQuery.nextSuperstep(workerIds, queryValueFactory);
						logger.info("Next master superstep query " + msgActiveQuery.Query.QueryId + ": " + msgActiveQuery.SuperstepNo);
						signalQueryNextSuperstep(msgActiveQuery.QueryAggregator, msgActiveQuery.SuperstepNo);
					}
					else {
						// All workers finished, finish query
						logger.info("All workers finished for query " + msgActiveQuery.Query.QueryId + ": " + msgActiveQuery.SuperstepNo +
								" after " + (System.currentTimeMillis() - msgActiveQuery.StartTime) + "ms");
						msgActiveQuery.workersFinished(workerIds);
						signalWorkersQueryFinish(msgActiveQuery.Query);
					}
				}
			}
			else { // Worker_Query_Finished
				if (msgActiveQuery.IsComputing) {
					logger.error("Query " + msgQueryOnWorker.QueryId + " still computing, wrong message: " + msgQueryOnWorker);
					return;
				}

				msgActiveQuery.workersWaitingFor.remove(message.getSrcMachine());
				logger.info("Worker " + message.getSrcMachine() + " finished query " + msgActiveQuery.Query.QueryId);

				if (msgActiveQuery.workersWaitingFor.isEmpty()) {
					// All workers have query finished
					logger.info("All workers finished query " + msgActiveQuery.Query.QueryId);
					evaluateQueryResult(msgActiveQuery);
					activeQueries.remove(msgActiveQuery.Query.QueryId);
					logger.info("Evaluated finished query " + msgActiveQuery.Query.QueryId);
				}
			}

			// TODO re-introduce?
			//			// Wrong message count should match broadcast message count, therwise there might be communication errors.
			//			if (workerIds.size() > 1
			//					&& ReceivedWrongVertexMessages != SentVertexMessagesBroadcast / (workerIds.size() - 1) * (workerIds.size() - 2)) {
			//				logger.warn(String.format(
			//						"Unexpected wrong vertex message count %d does not match broadcast message count %d. Should be %d. Possible communication errors.",
			//						ReceivedWrongVertexMessages, SentVertexMessagesBroadcast,
			//						SentVertexMessagesBroadcast / (workerIds.size() - 1) * (workerIds.size() - 2)));
			//			}
		}
		else {
			logger.error("Unexpected control message type in message " + message);
		}
	}


	@Override
	public void run() {
		// TODO Remove
	}


	private void evaluateQueryResult(ActiveQuery<Q> query) {
		// Aggregate output
		try {
			outputCombiner.evaluateOutput(outputDir + File.separator + query.Query.QueryId, query.Query);
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}
	}


	private void initializeWorkersAssignPartitions() {
		final Map<Integer, List<String>> assignedPartitions = inputPartitioner.partition(inputFile, inputPartitionDir, workerIds);
		for (final Integer workerId : workerIds) {
			messaging.sendControlMessageUnicast(workerId,
					ControlMessageBuildUtil.Build_Master_WorkerInitialize(ownId, assignedPartitions.get(workerId)), true);
		}
	}

	private void signalWorkersQueryStart(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryStart(ownId, query), true);
	}

	private void signalQueryNextSuperstep(Q query, int superstepNo) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryNextSuperstep(superstepNo, ownId, query),
				true);
	}

	private void signalWorkersQueryFinish(Q query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryFinish(ownId, query), true);
	}



	@Override
	public void onIncomingVertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag,
			List<Pair<Integer, NullWritable>> vertexMessages) {
		throw new RuntimeException("Master cannot handle vertex messages");
	}

	@Override
	public void onIncomingGetToKnowMessage(int srcMachine, Collection<Integer> vertices) {
		throw new RuntimeException("Master cannot handle GetToKnow messages");
	}
}
