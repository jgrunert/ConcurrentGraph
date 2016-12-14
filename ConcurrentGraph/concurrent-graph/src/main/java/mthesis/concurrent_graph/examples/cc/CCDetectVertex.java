package mthesis.concurrent_graph.examples.cc;

import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.vertex.VertexMessage;
import mthesis.concurrent_graph.vertex.VertexMessageSender;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex to detect connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class CCDetectVertex extends AbstractVertex<IntWritable, NullWritable, IntWritable> {
	private int value;
	private final Set<Integer> allNeighbors;


	public CCDetectVertex(int id, VertexMessageSender<IntWritable> messageSender) {
		super(id, messageSender);
		setValue(new IntWritable(id));
	}


	@Override
	protected void compute(List<VertexMessage<IntWritable>> messages) {
		if(superstepNo == 0) {
			//			for(final Integer nb : outgoingNeighbors) {
			//				System.out.println(superstepNo + " Send0 " + value + " to " + nb + " from " + id);
			//			}
			sendMessageToAllOutgoing(new IntWritable(ID));
			voteHalt();
			return;
		}

		int min = value;
		for(final VertexMessage<IntWritable> msg : messages) {
			allNeighbors.add(msg.SrcVertex);
			final int msgValue = msg.Content.Value;
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
		sendMessageToVertices(new IntWritable(value), allNeighbors);
	}


	@Override
	public String getOutput() {
		return Integer.toString(value);
	}


	public static class Factory extends VertexFactory<IntWritable, NullWritable, IntWritable> {

		@Override
		public AbstractVertex<IntWritable, NullWritable, IntWritable> newInstance(int id,
				VertexMessageSender<IntWritable> messageSender) {
			return new CCDetectVertex(id, messageSender);
		}
	}
}
