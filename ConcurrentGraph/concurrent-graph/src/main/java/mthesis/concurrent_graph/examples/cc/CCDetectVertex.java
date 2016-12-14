package mthesis.concurrent_graph.examples.cc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.Edge;
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

	// TODO Have this in state?
	private final Set<Integer> allNeighbors = new HashSet<>();

	public CCDetectVertex(int id, VertexMessageSender<IntWritable> messageSender) {
		super(id, messageSender);
		setValue(new IntWritable(id));
	}

	@Override
	protected void compute(List<VertexMessage<IntWritable>> messages) {
		if(superstepNo == 0) {
			final List<Edge<NullWritable>> edges = getEdges();
			for(final Edge<NullWritable> edge : edges) {
				allNeighbors.add(edge.TargetVertexId);
			}

			sendMessageToAllOutgoingEdges(getValue());
			voteHalt();
			return;
		}

		int min = getValue().Value;
		final int knownNeighborsBefore = allNeighbors.size();
		for(final VertexMessage<IntWritable> msg : messages) {
			allNeighbors.add(msg.SrcVertex);
			final int msgValue = msg.Content.Value;
			//System.out.println("Get " + msgValue + " on " + ID + " from " + msg.SrcVertex);
			min = Math.min(min, msgValue);
		}

		if(min < getValue().Value) {
			getValue().Value = min;
			sendMessageToVertices(getValue(), allNeighbors);
		} else {
			if(knownNeighborsBefore < allNeighbors.size())
				sendMessageToVertices(getValue(), allNeighbors);
			//System.out.println("Vote halt on " + ID + " with " + value);
			voteHalt();
		}
	}


	public static class Factory extends VertexFactory<IntWritable, NullWritable, IntWritable> {

		@Override
		public AbstractVertex<IntWritable, NullWritable, IntWritable> newInstance(int id,
				VertexMessageSender<IntWritable> messageSender) {
			return new CCDetectVertex(id, messageSender);
		}
	}
}
