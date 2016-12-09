package mthesis.concurrent_graph.master;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mthesis.concurrent_graph.communication.ControlMessageBuildUtil;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessageType;
import mthesis.concurrent_graph.node.AbstractNode;
import mthesis.concurrent_graph.util.Pair;

/**
 * Concurrent graph processing master main
 */
public class MasterNode extends AbstractNode {

	private final List<Integer> workerIds;
	private int superstepNo = -1;

	private final String inputData;
	private final String inputDir;
	private final String outputDir;
	private final Class<? extends AbstractMasterInputReader> inputReader;
	private final Class<? extends AbstractMasterOutputWriter> outputWriter;


	public MasterNode(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> workerIds,
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
				int messagesSent = 0;
				while(!workersWaitingFor.isEmpty()) {
					final ControlMessage msg = inControlMessages.take();
					if(msg.getType() == ControlMessageType.Worker_Superstep_Finished) {
						if(msg.getSuperstepNo() == superstepNo) {
							final int msgActiveVertices = msg.getContent1();
							if(msgActiveVertices > 0)
								activeWorkers++;
							activeVertices += msgActiveVertices;
							messagesSent += msg.getContent2();
							workersWaitingFor.remove(msg.getFromNode());
						}
						else {
							logger.error("Recieved Control_Worker_Superstep_Finished for wrong superstep: " + msg.getSuperstepNo() +
									" from " + msg.getFromNode());
						}
					}
					else if(msg.getType() == ControlMessageType.Worker_Finished) {
						// Finished
						logger.info("Received unexpected worker finish, terminate after " + (System.currentTimeMillis() - startTime) + "ms");
						break;
					}
					else {
						logger.error("Recieved non Control_Worker_Superstep_Finished message: " + msg.getType() +
								" from " + msg.getFromNode());
					}
				}

				if(activeWorkers > 0) {
					// Next superstep
					logger.debug(String.format("Master finished superstep %d after %dms. activeWorkers: %d activeVertices: %d messages: %d",
							superstepNo, (System.currentTimeMillis() - startTime), activeWorkers, activeVertices, messagesSent));
					// TODO Test
					System.out.println(String.format("Master finished superstep %d after %dms. activeWorkers: %d activeVertices: %d messages: %d",
							superstepNo, (System.currentTimeMillis() - startTime), activeWorkers, activeVertices, messagesSent));
					superstepNo++;
					logger.trace("Next master superstep: " + superstepNo);
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
					workersWaitingFor.remove(msg.getFromNode());
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
		System.out.println(superstepNo);
		messaging.sendControlMessage(workerIds, ControlMessageBuildUtil.Build_Master_Next_Superstep(superstepNo, ownId), true);
	}

	private void signalWorkersFinish() {
		messaging.sendControlMessage(workerIds, ControlMessageBuildUtil.Build_Master_Finish(superstepNo, ownId), true);
	}


	private static void makeCleanDirectory(String dir) {
		final File outFile = new File(dir);
		if(outFile.exists())
			for(final File f : outFile.listFiles())
				f.delete();
		else
			outFile.mkdirs();
	}
}
