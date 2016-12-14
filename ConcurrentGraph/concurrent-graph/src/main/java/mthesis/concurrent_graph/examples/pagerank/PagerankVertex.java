package mthesis.concurrent_graph.examples.pagerank;

import java.util.List;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.vertex.VertexMessage;
import mthesis.concurrent_graph.vertex.VertexMessageSender;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex for pagerank
 * 
 * @author Jonas Grunert
 *
 */
public class PagerankVertex extends AbstractVertex<DoubleWritable, NullWritable, DoubleWritable> {

	//	private double value;
	//	private final Set<Integer> allNeighbors;

	public PagerankVertex(int id, VertexMessageSender<DoubleWritable> messageSender) {
		super(id, messageSender);
	}

	@Override
	protected void compute(List<VertexMessage<DoubleWritable>> messages) {
		if(superstepNo == 0) {
			//			for(final Integer nb : outgoingNeighbors) {
			//				System.out.println(superstepNo + " Send0 " + value + " to " + nb + " from " + id);
			//			}
			sendMessageToAllOutgoing(new DoubleWritable(ID));
			return;
		}

		double min = getValue().Value;
		for(final VertexMessage<DoubleWritable> msg : messages) {
			allNeighbors.add(msg.SrcVertex);
			final double msgValue = msg.Content.Value;
			//			System.out.println(superstepNo + " Get " + msgValue + " on " + id + " from " + msg.FromVertex);
			min = Math.min(min, msgValue);
		}

		if(min < getValue().Value) {
			//			System.out.println(superstepNo + " Update on " + id + " to " + min);
			getValue().Value = min;
		} else {
			//			System.out.println(superstepNo + " Vote halt on " + id + " with " + value);
			voteHalt();
		}

		//		for(final Integer nb : allNeighbors) {
		//			System.out.println(superstepNo + " Send " + value + " to " + nb + " from " + id);
		//		}
		sendMessageToVertices(getValue(), allNeighbors);
	}


	@Override
	public String getOutput() {
		return Double.toString(value);
	}


	public static class Factory extends VertexFactory<DoubleWritable, NullWritable, DoubleWritable> {

		@Override
		public AbstractVertex<DoubleWritable, NullWritable, DoubleWritable> newInstance(int id,
				VertexMessageSender<DoubleWritable> messageSender) {
			return new PagerankVertex(id, messageSender);
		}
	}
}
