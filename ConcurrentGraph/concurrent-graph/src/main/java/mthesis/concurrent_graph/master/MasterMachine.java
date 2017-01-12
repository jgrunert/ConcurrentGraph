package mthesis.concurrent_graph.master;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
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
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage;
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
 * @param <G>
 *            Global query values
 */
public class MasterMachine<G extends BaseQueryGlobalValues> extends AbstractMachine<NullWritable> {

	private final List<Integer> workerIds;
	//private volatile boolean workersInitialized = false;
	private int superstepNo = -1;

	private final String inputFile;
	private final String inputPartitionDir;
	private final String outputDir;
	private int vertexCount;
	private final MasterInputPartitioner inputPartitioner;
	private final MasterOutputEvaluator<G> outputCombiner;

	private final BaseQueryGlobalValuesFactory<G> globalValueFactory;
	private G activeQuery;


	public MasterMachine(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, String inputFile,
			String inputPartitionDir, MasterInputPartitioner inputPartitioner, MasterOutputEvaluator<G> outputCombiner, String outputDir,
			BaseQueryGlobalValuesFactory<G> globalValueFactory) {
		super(machines, ownId, null);
		this.workerIds = workerIds;
		this.inputFile = inputFile;
		this.inputPartitionDir = inputPartitionDir;
		this.inputPartitioner = inputPartitioner;
		this.outputCombiner = outputCombiner;
		this.outputDir = outputDir;
		this.globalValueFactory = globalValueFactory;
		FileUtil.makeCleanDirectory(outputDir);
	}


	public void startQuery(G query) {
		if (activeQuery != null) throw new RuntimeException("TODO");
		FileUtil.makeCleanDirectory(outputDir + File.separator + Integer.toString(query.QueryId));
		activeQuery = query;
		// TODO Start on workers
	}


	@Override
	public void run() {
		logger.info("Master started");
		final long masterStartTime = System.currentTimeMillis();
		long lastSuperstepTime = masterStartTime; // TORO Multi query

		try {
			// Initialize workers
			superstepNo = 0;
			initializeWorkersAssignPartitions(); // Signal workers to initialize
			waitForWorkersInitialized(); // Wait for workers to finish initialize
			logger.info("Workers initialized after " + (System.currentTimeMillis() - masterStartTime) + "ms");

			// Process queries
			boolean processingQuery = false;
			final Set<Integer> workersWaitingFor = new HashSet<>(workerIds.size());
			while (!Thread.interrupted()) {
				// TODO Better wait for query
				if (activeQuery == null) {
					Thread.sleep(100);
					continue;
				}

				if (!processingQuery) {
					// Start new query
					activeQuery.setVertexCount(vertexCount);
					signalWorkersQueryStart(activeQuery);
					logger.info("Master started query " + activeQuery.QueryId);
					processingQuery = true;
				}

				// New superstep
				workersWaitingFor.addAll(workerIds);

				// Wait for workers to send finished control messages.
				int activeWorkers = 0;
				int SentControlMessages = 0;
				int SentVertexMessagesLocal = 0;
				int SentVertexMessagesUnicast = 0;
				int SentVertexMessagesBroadcast = 0;
				int SentVertexMessagesBuckets = 0;
				int ReceivedCorrectVertexMessages = 0;
				int ReceivedWrongVertexMessages = 0;
				int newVertexMachinesDiscovered = 0;
				int totalVertexMachinesDiscovered = 0;
				G aggregatedGlobalValues = globalValueFactory.createClone(activeQuery);
				while (!workersWaitingFor.isEmpty()) {
					final ControlMessage msg = inControlMessages.take();
					if (msg.getType() == ControlMessageType.Worker_Query_Superstep_Finished) {
						if (msg.getSuperstepNo() == superstepNo) {
							final WorkerStatsMessage workerStats = msg.getWorkerStats();

							G workerValues = globalValueFactory.createFromBytes(ByteBuffer.wrap(msg.getQueryValues().toByteArray()));
							if (workerValues.getActiveVertices() > 0) activeWorkers++;
							aggregatedGlobalValues.add(workerValues);

							SentControlMessages += workerStats.getSentControlMessages();
							SentVertexMessagesLocal += workerStats.getSentVertexMessagesLocal();
							SentVertexMessagesUnicast += workerStats.getSentVertexMessagesUnicast();
							SentVertexMessagesBroadcast += workerStats.getSentVertexMessagesBroadcast();
							SentVertexMessagesBuckets += workerStats.getSentVertexMessagesBuckets();
							ReceivedCorrectVertexMessages += workerStats.getReceivedCorrectVertexMessages();
							ReceivedWrongVertexMessages += workerStats.getReceivedWrongVertexMessages();
							newVertexMachinesDiscovered += workerStats.getNewVertexMachinesDiscovered();
							totalVertexMachinesDiscovered += workerStats.getTotalVertexMachinesDiscovered();
							workersWaitingFor.remove(msg.getSrcMachine());
						}
						else {
							logger.error("Recieved Control_Worker_Superstep_Finished for wrong superstep: " + msg.getSuperstepNo()
									+ " from " + msg.getSrcMachine());
						}
					}
					//					else if (msg.getType() == ControlMessageType.Worker_Query_Finished) {
					//						// Finished
					//						logger.warn(
					//								"Received unexpected worker finish, terminate after " + (System.currentTimeMillis() - startTime) + "ms");
					//						break;
					//					}
					else {
						logger.error("Recieved non Control_Worker_Superstep_Finished message: " + msg.getType() + " from "
								+ msg.getSrcMachine());
					}
				}

				// Wrong message count should match broadcast message count.
				// Otherwise there might be communication errors.
				if (workerIds.size() > 1
						&& ReceivedWrongVertexMessages != SentVertexMessagesBroadcast / (workerIds.size() - 1) * (workerIds.size() - 2)) {
					logger.warn(String.format(
							"Unexpected wrong vertex message count %d does not match broadcast message count %d. Should be %d. Possible communication errors.",
							ReceivedWrongVertexMessages, SentVertexMessagesBroadcast,
							SentVertexMessagesBroadcast / (workerIds.size() - 1) * (workerIds.size() - 2)));
				}

				final long timeNow = System.currentTimeMillis();
				System.out.println("----- superstep " + superstepNo + " -----");
				logger.info(String.format("- Master finished superstep %d after %dms (total %dms). activeWorkers: %d activeVertices: %d",
						superstepNo, (timeNow - lastSuperstepTime), (timeNow - masterStartTime), activeWorkers, // Query time
						aggregatedGlobalValues.getActiveVertices()));
				logger.info(String.format(
						"  SentControlMessages: %d, SentVertexMessagesLocal: %d, SentVertexMessagesUnicast: %d, SentVertexMessagesBroadcast: %d, SentVertexMessagesBuckets %d",
						SentControlMessages, SentVertexMessagesLocal, SentVertexMessagesUnicast, SentVertexMessagesBroadcast,
						SentVertexMessagesBuckets));
				logger.info(String.format("  ReceivedCorrectVertexMessages: %d, ReceivedWrongVertexMessages: %d",
						ReceivedCorrectVertexMessages, ReceivedWrongVertexMessages));
				logger.info(String.format("  newVertexMachinesDiscovered: %d, totalVertexMachinesDiscovered: %d",
						newVertexMachinesDiscovered, totalVertexMachinesDiscovered));
				lastSuperstepTime = timeNow;

				if (activeWorkers > 0) {
					// Next query superstep
					superstepNo++;
					logger.info("Next master superstep: " + superstepNo);
					signalQueryNextSuperstep(aggregatedGlobalValues);
				}
				else {
					// Finished query
					logger.info("All workers finished after " + (System.currentTimeMillis() - masterStartTime) + "ms"); // TODO Query time
					processingQuery = false;
					finishQueryWorkers(activeQuery);
					evaluateQueryResult(activeQuery);
					activeQuery = null;
					// TODO Evaluate time
				}
			}
		}
		catch (final InterruptedException e) {
			logger.info("Master interrupted");
		}
		catch (final Exception e) {
			logger.error("Master exception", e);
		}
		finally {
			// End sequence: Let workers finish and output, then finish master before terminating
			logger.info("Master terminating after " + (System.currentTimeMillis() - masterStartTime) + "ms");
			stop();
		}
	}


	// Wait for workers initialized before starting
	private void waitForWorkersInitialized() {
		try {
			final Set<Integer> workersWaitingFor = new HashSet<>(workerIds);
			vertexCount = 0;
			while (!workersWaitingFor.isEmpty()) {
				ControlMessage msg;
				msg = inControlMessages.take();
				if (msg.getType() == ControlMessageType.Worker_Initialized) {
					workersWaitingFor.remove(msg.getSrcMachine());
					vertexCount += msg.getWorkerInitialized().getVertexCount();
					logger.debug("Worker initialized: " + msg.getSrcMachine());
				}
				else {
					logger.debug("waitForWorkersInitialized wrong message: " + msg.getType());
				}
			}
			logger.info("All workers initialized, " + workerIds.size() + " workers, " + vertexCount + " vertices");
			//workersInitialized = true;
		}
		catch (final Exception e) {
			logger.error("Exception while waitForWorkersInitialized", e);
		}
	}

	// Wait for workers to finish queries
	private void finishQueryWorkers(G query) {
		signalWorkersQueryFinish(query);
		try {
			final Set<Integer> workersWaitingFor = new HashSet<>(workerIds);
			while (!workersWaitingFor.isEmpty()) {
				ControlMessage msg;
				msg = inControlMessages.take();
				if (msg.getType() == ControlMessageType.Worker_Query_Finished) {
					workersWaitingFor.remove(msg.getSrcMachine());
				}
			}
		}
		catch (final Exception e) {
			logger.error("Exception while finishWorkers", e);
		}
	}

	private void evaluateQueryResult(G query) {
		// Aggregate output
		try {
			outputCombiner.evaluateOutput(outputDir + File.separator + query.QueryId, query);
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}
	}


	private void initializeWorkersAssignPartitions() {
		final Map<Integer, List<String>> assignedPartitions = inputPartitioner.partition(inputFile, inputPartitionDir, workerIds);
		for (final Integer workerId : workerIds) {
			messaging.sendControlMessageUnicast(workerId,
					ControlMessageBuildUtil.Build_Master_WorkerInitialize(superstepNo, ownId, assignedPartitions.get(workerId)), true);
		}
	}

	private void signalWorkersQueryStart(G query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryStart(ownId, query), true);
	}

	private void signalQueryNextSuperstep(G query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryNextSuperstep(superstepNo, ownId, query),
				true);
	}

	private void signalWorkersQueryFinish(G query) {
		messaging.sendControlMessageMulticast(workerIds, ControlMessageBuildUtil.Build_Master_QueryFinish(ownId, query), true);
	}



	@Override
	public void onIncomingVertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag,
			List<Pair<Integer, NullWritable>> vertexMessages) {
		throw new RuntimeException("Master cannot handle vertex messages");
	}


	@Override
	public void onIncomingGetToKnowMessage(int srcMachine, Collection<Integer> vertices) {
		throw new RuntimeException("Master cannot handle vertex messages");
	}
}
