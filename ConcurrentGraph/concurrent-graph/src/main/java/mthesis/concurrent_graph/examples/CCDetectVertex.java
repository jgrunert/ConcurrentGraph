package mthesis.concurrent_graph.examples;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.communication.Messages.VertexMessage;
import mthesis.concurrent_graph.vertex.AbstractVertex;
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
			//				System.out.println(superstepNo + " Send0 " + value + " to " + nb + " from " + id);
			//			}
			sendMessageToAllOutgoing(id);
			return;
		}

		int min = value;
		for(final VertexMessage msg : messages) {
			allNeighbors.add(msg.getFromVertex());
			final int msgValue = msg.getContent();
			//			System.out.println(superstepNo + " Get " + msgValue + " on " + id + " from " + msg.FromVertex);
			min = Math.min(min, msgValue);
		}

		if(min < value) {
			//			System.out.println(superstepNo + " Update on " + id + " to " + min);
			value = min;
		} else {
			//			System.out.println(superstepNo + " Vote halt on " + id + " with " + value);
			voteHalt();
		}

		//		for(final Integer nb : allNeighbors) {
		//			System.out.println(superstepNo + " Send " + value + " to " + nb + " from " + id);
		//		}
		sendMessageToVertices(value, allNeighbors);
	}


	@Override
	public String getOutput() {
		return Integer.toString(value);
	}
}
