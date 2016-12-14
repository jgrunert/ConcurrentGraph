package mthesis.concurrent_graph.examples.cc;

import java.util.List;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.vertex.VertexMessage;
import mthesis.concurrent_graph.vertex.VertexMessageSender;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex to detect strongly connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class SCCDetectVertex extends AbstractVertex<IntWritable, NullWritable, IntWritable> {

	public SCCDetectVertex(int id, VertexMessageSender<IntWritable> messageSender) {
		super(id, messageSender);
		setValue(new IntWritable(id));
	}

	@Override
	protected void compute(List<VertexMessage<IntWritable>> messages) {
		if(superstepNo == 0) {
			sendMessageToAllOutgoingEdges(getValue());
			voteHalt();
			return;
		}

		int min = getValue().Value;
		for(final VertexMessage<IntWritable> msg : messages) {
			final int msgValue = msg.Content.Value;
			//System.out.println("Get " + msgValue + " on " + ID + " from " + msg.SrcVertex);
			min = Math.min(min, msgValue);
		}

		if(min < getValue().Value) {
			getValue().Value = min;
			sendMessageToAllOutgoingEdges(getValue());
		} else {
			//System.out.println("Vote halt on " + ID + " with " + value);
			voteHalt();
		}
	}


	public static class Factory extends VertexFactory<IntWritable, NullWritable, IntWritable> {

		@Override
		public AbstractVertex<IntWritable, NullWritable, IntWritable> newInstance(int id,
				VertexMessageSender<IntWritable> messageSender) {
			return new SCCDetectVertex(id, messageSender);
		}
	}
}
