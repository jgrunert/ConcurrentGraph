package mthesis.concurrent_graph.vertex;

import java.util.List;

import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.worker.WorkerNode;

/**
 * Example vertex to detect strongly connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class SCCDetectVertex extends AbstractVertex {

	private int value;

	public SCCDetectVertex(List<Integer> neighbors, int id, WorkerNode workerManager) {
		super(neighbors, id, workerManager);
		value = id;
	}

	@Override
	protected void compute(List<VertexMessage> messages) {
		if(superstepNo == 0) {
			sendMessageToAllOutgoing(Integer.toString(id));
			return;
		}

		int min = value;
		for(final VertexMessage msg : messages) {
			final int msgValue = Integer.parseInt(msg.Content);
			System.out.println("Get " + msgValue + " on " + id + " from " + msg.FromVertex);
			min = Math.min(min, msgValue);
		}

		if(min < value) {
			value = min;
			sendMessageToAllOutgoing(Integer.toString(value));
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
