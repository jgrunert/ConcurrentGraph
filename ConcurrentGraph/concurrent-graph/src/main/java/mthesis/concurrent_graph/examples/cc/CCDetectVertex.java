package mthesis.concurrent_graph.examples.cc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.Edge;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex to detect connected components in a graph
 * 
 * @author Jonas Grunert
 *
 */
public class CCDetectVertex extends AbstractVertex<IntWritable, NullWritable, CCMessageWritable, QueryGlobalValues> {

	// TODO Have this in state?
	private final Set<Integer> allNeighbors = new HashSet<>();

	public CCDetectVertex(int id, VertexWorkerInterface<CCMessageWritable, QueryGlobalValues> messageSender) {
		super(id, messageSender);
		setValue(new IntWritable(id));
	}

	@Override
	protected void compute(List<CCMessageWritable> messages) {
		if (superstepNo == 0) {
			final List<Edge<NullWritable>> edges = getEdges();
			for (final Edge<NullWritable> edge : edges) {
				allNeighbors.add(edge.TargetVertexId);
			}

			sendMessageToAllOutgoingEdges(new CCMessageWritable(ID, ID));
			voteHalt();
			return;
		}

		int min = getValue().Value;
		final int knownNeighborsBefore = allNeighbors.size();
		for (final CCMessageWritable msg : messages) {
			allNeighbors.add(msg.SrcVertex);
			final int msgValue = msg.Value;
			//System.out.println("Get " + msgValue + " on " + ID + " from " + msg.SrcVertex);
			min = Math.min(min, msgValue);
		}

		if (min < getValue().Value) {
			getValue().Value = min;
			sendMessageToVertices(new CCMessageWritable(ID, getValue().Value), allNeighbors);
		}
		else {
			if (knownNeighborsBefore < allNeighbors.size())
				sendMessageToVertices(new CCMessageWritable(ID, getValue().Value), allNeighbors);
			//System.out.println("Vote halt on " + ID + " with " + value);
			voteHalt();
		}
	}


	public static class Factory extends VertexFactory<IntWritable, NullWritable, CCMessageWritable, QueryGlobalValues> {

		@Override
		public AbstractVertex<IntWritable, NullWritable, CCMessageWritable, QueryGlobalValues> newInstance(int id,
				VertexWorkerInterface<CCMessageWritable, QueryGlobalValues> messageSender) {
			return new CCDetectVertex(id, messageSender);
		}
	}
}
