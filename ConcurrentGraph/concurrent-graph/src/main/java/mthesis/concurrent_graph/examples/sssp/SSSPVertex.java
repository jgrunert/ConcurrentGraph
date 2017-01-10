package mthesis.concurrent_graph.examples.sssp;

import java.util.List;

import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.Edge;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.DoubleWritable;

/**
 * Example vertex for single source shortest path
 * 
 * @author Jonas Grunert
 *
 */
public class SSSPVertex extends AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, QueryGlobalValues> {

	public SSSPVertex(int id, VertexWorkerInterface<SSSPMessageWritable, QueryGlobalValues> messageSender) {
		super(id, messageSender);
		setValue(new SSSPVertexWritable(-1, Double.POSITIVE_INFINITY));
	}

	@Override
	protected void compute(List<SSSPMessageWritable> messages) {
		final SSSPVertexWritable mutableValue = getValue();
		double minDist = mutableValue.Dist;
		int minPre = mutableValue.Pre;
		for (SSSPMessageWritable msg : messages) {
			if (msg.Dist < minDist) {
				minDist = msg.Dist;
				minPre = msg.SrcVertex;
			}
		}

		if (minDist < mutableValue.Dist) {
			mutableValue.Dist = minDist;
			mutableValue.Pre = minPre;
			for (Edge<DoubleWritable> edge : getEdges()) {
				sendMessageToVertex(new SSSPMessageWritable(mutableValue.Pre, mutableValue.Dist + edge.Value.Value), edge.TargetVertexId);
			}
		}
		voteHalt();
	}


	public static class Factory extends VertexFactory<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, QueryGlobalValues> {

		@Override
		public AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, QueryGlobalValues> newInstance(int id,
				VertexWorkerInterface<SSSPMessageWritable, QueryGlobalValues> messageSender) {
			return new SSSPVertex(id, messageSender);
		}
	}
}
