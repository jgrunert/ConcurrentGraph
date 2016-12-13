package mthesis.concurrent_graph.master;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.communication.Messages.VertexMessage;
import mthesis.concurrent_graph.node.AbstractMachine;
import mthesis.concurrent_graph.util.Pair;

/**
 * Concurrent graph processing master main
 */
public class MasterMachine extends AbstractMachine {

	private final List<Integer> workerIds;
	private int superstepNo = -1;

	private final String inputData;
	private final String inputDir;
	private final String outputDir;
	private final Class<? extends AbstractMasterInputReader> inputReader;
	private final Class<? extends AbstractMasterOutputWriter> outputWriter;


	public MasterMachine(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> workerIds,
			String inputData, String inputDir, String outputDir,
			Class<? extends AbstractMasterInputReader> inputReader,
			Class<? extends AbstractMasterOutputWriter> outputWriter) {
		super(machines, ownId);
		this.workerIds = workerIds;
		this.inputData = inputData;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.inputReader = inputReader;
		this.outputWriter = outputWriter;
		makeCleanDirectory(inputDir);
		makeCleanDirectory(outputDir);
	}


	@Override
	public void run() {
		logger.info("Master started");
		final long startTime = System.currentTimeMillis();
		long lastSuperstepTime = startTime;

		try {
			inputReader.newInstance().readAndPartition(inputData, inputDir, workerIds.size());
			logger.info("Master input read and partitioned after " + (System.currentTimeMillis() - startTime) + "ms");
			superstepNo = -1;
			signalWorkersStartingSuperstep();  // Signal that input ready

			final Set<Integer> workersWaitingFor = new HashSet<>(workerIds.size());
			while(!Thread.interrupted()) {
				// New superstep
				workersWaitingFor.addAll(workerIds);

				// Wait for workers to send finished control messages.
				int activeWorkers = 0;
				int activeVertices = 0;
				int SentControlMessages = 0;
				int SentVertexMessagesLocal = 0;
				int SentVertexMessagesUnicast = 0;
				int SentVertexMessagesBroadcast = 0;
				int ReceivedCorrectVertexMessages = 0;
				int ReceivedWrongVertexMessages = 0;
				int newVertexMachinesDiscovered = 0;
				int totalVertexMachinesDiscovered = 0;
				while(!workersWaitingFor.isEmpty()) {
					final ControlMessage msg = inControlMessages.take();
					if(msg.getType() == ControlMessageType.Worker_Superstep_Finished) {
						if(msg.getSuperstepNo() == superstepNo) {
							final WorkerStatsMessage workerStats = msg.getWorkerStats();
							if(workerStats.getActiveVertices() > 0)
								activeWorkers++;
							activeVertices += workerStats.getActiveVertices();
							SentControlMessages += workerStats.getSentControlMessages();
							SentVertexMessagesLocal += workerStats.getSentVertexMessagesLocal();
							SentVertexMessagesUnicast += workerStats.getSentVertexMessagesUnicast();
							SentVertexMessagesBroadcast += workerStats.getSentVertexMessagesBroadcast();
							ReceivedCorrectVertexMessages += workerStats.getReceivedCorrectVertexMessages();
							ReceivedWrongVertexMessages += workerStats.getReceivedWrongVertexMessages();
							newVertexMachinesDiscovered += workerStats.getNewVertexMachinesDiscovered();
							totalVertexMachinesDiscovered += workerStats.getTotalVertexMachinesDiscovered();
							workersWaitingFor.remove(msg.getSrcMachine());
						}
						else {
							logger.error("Recieved Control_Worker_Superstep_Finished for wrong superstep: " + msg.getSuperstepNo() +
									" from " + msg.getSrcMachine());
						}
					}
					else if(msg.getType() == ControlMessageType.Worker_Finished) {
						// Finished
						logger.warn("Received unexpected worker finish, terminate after " + (System.currentTimeMillis() - startTime) + "ms");
						break;
					}
					else {
						logger.error("Recieved non Control_Worker_Superstep_Finished message: " + msg.getType() +
								" from " + msg.getSrcMachine());
					}
				}

				// Wrong message count should match broadcast message count.
				// Otherwise there might be communication errors.
				if(ReceivedWrongVertexMessages != SentVertexMessagesBroadcast * (workerIds.size() - 2) / (workerIds.size() - 1)) {
					logger.warn(String.format("Wrong vertex message count %d does not match broadcast message count %d. Possible communication errors.",
							ReceivedWrongVertexMessages, SentVertexMessagesBroadcast));
				}

				final long timeNow = System.currentTimeMillis();
				System.out.println("----- superstep " + superstepNo + " -----");
				logger.info(String.format("--- Master finished superstep %d after %dms (total %dms). activeWorkers: %d activeVertices: %d",
						superstepNo, (timeNow - lastSuperstepTime), (timeNow - startTime), activeWorkers, activeVertices));
				logger.info(String.format("    SentControlMessages: %d, SentVertexMessagesLocal: %d, SentVertexMessagesUnicast: %d, SentVertexMessagesBroadcast: %d",
						SentControlMessages, SentVertexMessagesLocal, SentVertexMessagesUnicast, SentVertexMessagesBroadcast));
				logger.info(String.format("    ReceivedCorrectVertexMessages: %d, ReceivedWrongVertexMessages: %d",
						ReceivedCorrectVertexMessages, ReceivedWrongVertexMessages));
				logger.info(String.format("    newVertexMachinesDiscovered: %d, totalVertexMachinesDiscovered: %d",
						newVertexMachinesDiscovered, totalVertexMachinesDiscovered));
				lastSuperstepTime = timeNow;

				if(activeWorkers > 0) {
					// Next superstep
					superstepNo++;
					logger.info("Next master superstep: " + superstepNo);
					signalWorkersStartingSuperstep();
				}
				else {
					// Finished
					logger.info("All workers finished after " + (System.currentTimeMillis() - startTime) + "ms");
					break;
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
			logger.info("Master finishing after " + (System.currentTimeMillis() - startTime) + "ms");
			finishWorkers();
			finishMaster();
			logger.info("Master terminating after " + (System.currentTimeMillis() - startTime) + "ms");
			stop();
		}
	}


	private void finishWorkers() {
		signalWorkersFinish();

		try {
			// Wait for workers to finish
			final Set<Integer> workersWaitingFor = new HashSet<>(workerIds);
			while(!workersWaitingFor.isEmpty()) {
				ControlMessage msg;
				msg = inControlMessages.take();
				if(msg.getType() == ControlMessageType.Worker_Finished) {
					workersWaitingFor.remove(msg.getSrcMachine());
				}
			}
		}
		catch (final Exception e) {
			logger.error("Exception while finishing", e);
		}
	}

	private void finishMaster() {
		// Aggregate output
		try
		{
			outputWriter.newInstance().writeOutput(outputDir);
		}
		catch(final Exception e)
		{
			logger.error("writeOutput failed", e);
		}
	}


	private void signalWorkersStartingSuperstep() {
		messaging.sendMessageBroadcast(workerIds, ControlMessageBuildUtil.Build_Master_Next_Superstep(superstepNo, ownId), true);
	}

	private void signalWorkersFinish() {
		messaging.sendMessageBroadcast(workerIds, ControlMessageBuildUtil.Build_Master_Finish(superstepNo, ownId), true);
	}


	private static void makeCleanDirectory(String dir) {
		final File outFile = new File(dir);
		if(outFile.exists())
			for(final File f : outFile.listFiles())
				f.delete();
		else
			outFile.mkdirs();
	}


	@Override
	public void onIncomingVertexMessage(VertexMessage message) {
		throw new RuntimeException("Master cannot handle vertex messages");
	}
}
