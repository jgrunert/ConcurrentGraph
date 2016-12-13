package mthesis.concurrent_graph.examples;

import java.util.List;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexMessage;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.IntWritable;

/**
 * Example vertex to detect strongly connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class SCCDetectVertex extends AbstractVertex<IntWritable, IntWritable> {

	private int value;

	public SCCDetectVertex(List<Integer> neighbors, int id, WorkerMachine<IntWritable, IntWritable> workerManager) {
		super(neighbors, id, workerManager);
		value = id;
	}

	@Override
	protected void compute(List<VertexMessage<IntWritable>> messages) {
		if(superstepNo == 0) {
			sendMessageToAllOutgoing(new IntWritable(id));
			return;
		}

		int min = value;
		for(final VertexMessage<IntWritable> msg : messages) {
			final int msgValue = msg.Content.Value;
			System.out.println("Get " + msgValue + " on " + id + " from " + msg.SrcVertex);
			min = Math.min(min, msgValue);
		}

		if(min < value) {
			value = min;
			sendMessageToAllOutgoing(new IntWritable(value));
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
