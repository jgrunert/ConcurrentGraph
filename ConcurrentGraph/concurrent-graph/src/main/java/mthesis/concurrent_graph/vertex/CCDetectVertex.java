package mthesis.concurrent_graph.vertex;

import java.util.List;

import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.worker.WorkerNode;

/**
 * Example vertex to detect connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class CCDetectVertex extends AbstractVertex {

	private int value;

	public CCDetectVertex(List<Integer> neighbors, int id, WorkerNode workerManager) {
		super(neighbors, id, workerManager);
		value = id;
	}

	@Override
	public void compute(List<VertexMessage> messages) {
		if(superstepNo == 0) {
			sendMessageToAllNeighbors(Integer.toString(value));
		}

		int min = value;
		for(final VertexMessage msg : messages) {
			final int msgValue = Integer.parseInt(msg.Content);
			min = Math.min(min, msgValue);
		}

		if(min < value) {
			value = min;
			sendMessageToAllNeighbors(Integer.toString(value));
		}
	}
}
