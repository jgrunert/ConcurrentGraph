package mthesis.concurrent_graph.examples;

import java.util.List;

import mthesis.concurrent_graph.AbstractVertex;
import mthesis.concurrent_graph.communication.Messages.VertexMessage;
import mthesis.concurrent_graph.worker.WorkerMachine;

/**
 * Example vertex to detect strongly connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class SCCDetectVertex extends AbstractVertex<CCDetectVertexValue, CCDetectVertexMessage> {

	private int value;

	public SCCDetectVertex(List<Integer> neighbors, int id, WorkerMachine workerManager) {
		super(neighbors, id, workerManager);
		value = id;
	}

	@Override
	protected void compute(List<VertexMessage> messages) {
		if(superstepNo == 0) {
			sendMessageToAllOutgoing(id);
			return;
		}

		int min = value;
		for(final VertexMessage msg : messages) {
			final int msgValue = msg.getContent();
			System.out.println("Get " + msgValue + " on " + id + " from " + msg.getSrcVertex());
			min = Math.min(min, msgValue);
		}

		if(min < value) {
			value = min;
			sendMessageToAllOutgoing(value);
		} else {
			System.out.println("Vote halt on " + id + " with " + value);
			voteHalt();
		}
	}


	@Override
	public String getOutput() {
		return Integer.toString(value);
	}
}
