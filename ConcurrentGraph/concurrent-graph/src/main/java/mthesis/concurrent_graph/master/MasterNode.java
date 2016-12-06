package mthesis.concurrent_graph.master;

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


	public MasterNode(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> workerIds) {
		super(machines, ownId);
		this.workerIds = workerIds;
	}


	@Override
	public void run() {
		logger.info("Master started");

		final Set<Integer> workersWaitingFor = new HashSet<>(workerIds.size());
		superstepNo = -1;

		try {
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
					signalWorkersNextSuperstep();
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
		finally {
			logger.info("Master shutting down");
			signalWorkersTerminate();
			stop();
		}
	}


	private void signalWorkersNextSuperstep() {
		messaging.sendMessageTo(workerIds, MessageType.Control_Master_Next_Superstep + ";" + ownId + ";" + superstepNo + ";" + "next");
	}

	private void signalWorkersTerminate() {
		messaging.sendMessageTo(workerIds, MessageType.Control_Master_Terminate + ";" + ownId + ";" + superstepNo + ";" + "terminate");
	}
}
