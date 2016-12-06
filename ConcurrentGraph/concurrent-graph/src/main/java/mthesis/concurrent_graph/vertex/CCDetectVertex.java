package mthesis.concurrent_graph.vertex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
	private final Set<Integer> allNeighbors;

	public CCDetectVertex(List<Integer> neighbors, int id, WorkerNode workerManager) {
		super(neighbors, id, workerManager);
		allNeighbors = new HashSet<>(neighbors);
		value = id;
	}

	@Override
	protected void compute(List<VertexMessage> messages) {
		if(superstepNo == 0) {
			//			for(final Integer nb : outgoingNeighbors) {
			//				System.out.println("Send0 " + value + " to " + nb + " from " + id);
			//			}
			sendMessageToAllOutgoing(Integer.toString(id));
			return;
		}

		int min = value;
		for(final VertexMessage msg : messages) {
			allNeighbors.add(msg.FromVertex);
			final int msgValue = Integer.parseInt(msg.Content);
			//			System.out.println("Get " + msgValue + " on " + id + " from " + msg.FromVertex);
			min = Math.min(min, msgValue);
		}

		if(min < value) {
			//			System.out.println("Update on " + id + " to " + min);
			value = min;
		} else {
			//			System.out.println("Vote halt on " + id + " with " + value);
			voteHalt();
		}

		//		for(final Integer nb : allNeighbors) {
		//			System.out.println("Send " + value + " to " + nb + " from " + id);
		//		}
		sendMessageToVertices(Integer.toString(value), allNeighbors);
	}


	@Override
	public String getOutput() {
		return Integer.toString(value);
	}
}
