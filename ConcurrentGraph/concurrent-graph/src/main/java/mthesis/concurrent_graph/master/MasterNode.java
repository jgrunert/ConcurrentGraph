package mthesis.concurrent_graph.master;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mthesis.concurrent_graph.communication.ControlMessage;
import mthesis.concurrent_graph.communication.MessageType;
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

		try {
			inputReader.newInstance().readAndPartition(inputData, inputDir, workerIds.size());
			logger.info("Master input read and partitioned");
			superstepNo = -1;
			signalWorkersStartingSuperstep();  // Signal that input ready

			final Set<Integer> workersWaitingFor = new HashSet<>(workerIds.size());
			while(!Thread.interrupted()) {
				// New superstep
				workersWaitingFor.addAll(workerIds);

				// Wait for workers to send finished control messages.
				int activeWorkers = 0;
				//int activeVertices = 0;
				while(!workersWaitingFor.isEmpty()) {
					final ControlMessage msg = inControlMessages.take();
					if(msg.Type == MessageType.Control_Node_Superstep_Finished) {
						if(msg.SuperstepNo == superstepNo) {
							final int msgActiveVertices = Integer.parseInt(msg.Content);
							if(msgActiveVertices > 0)
								activeWorkers++;
							//activeVertices += msgActiveVertices;
							workersWaitingFor.remove(msg.FromNode);
						}
						else {
							logger.info("Recieved Control_Node_Superstep_Finished for wrong superstep: " + msg.SuperstepNo +
									" from " + msg.FromNode);
						}
					}
					else {
						logger.info("Recieved non Control_Node_Superstep_Finished message: " + msg.Type +
								" from " + msg.FromNode);
					}
				}

				if(activeWorkers > 0) {
					// Next superstep
					superstepNo++;
					logger.trace("Next master superstep: " + superstepNo);
					signalWorkersStartingSuperstep();
				}
				else {
					// Finished
					logger.info("All workers finished");
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
			logger.info("Master finishing");
			finishWorkers();
			finishMaster();
			logger.info("Master terminating");
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
				if(msg.Type == MessageType.Control_Node_Finished) {
					workersWaitingFor.remove(msg.FromNode);
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
		messaging.sendMessageTo(workerIds, MessageType.Control_Master_Next_Superstep + ";" + ownId + ";" + superstepNo + ";" + "next");
	}

	private void signalWorkersFinish() {
		messaging.sendMessageTo(workerIds, MessageType.Control_Master_Finish + ";" + ownId + ";" + superstepNo + ";" + "terminate");
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
