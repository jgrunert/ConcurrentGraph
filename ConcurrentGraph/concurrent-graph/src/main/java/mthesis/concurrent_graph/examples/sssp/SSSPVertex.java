package mthesis.concurrent_graph.examples.sssp;

import java.util.List;

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
public class SSSPVertex extends AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> {

	public SSSPVertex(int id,
			VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> messageSender) {
		super(id, messageSender);
	}

	@Override
	protected void compute(int superstepNo, List<SSSPMessageWritable> messages, SSSPQueryValues query) {
		if (superstepNo == 0) {
			if (ID != query.From) {
				voteVertexHalt();
				return;
			}
			else {
				System.out.println("GO " + ID);
				SSSPVertexWritable mutableValue = new SSSPVertexWritable(-1, 0);
				setValue(mutableValue, query.QueryId);
				for (Edge<DoubleWritable> edge : getEdges()) {
					sendMessageToVertex(new SSSPMessageWritable(ID, edge.Value.Value), edge.TargetVertexId);
				}
				return;
			}
		}

		SSSPVertexWritable mutableValue = getValue(query.QueryId);
		if (mutableValue == null) {
			mutableValue = new SSSPVertexWritable(-1, Double.POSITIVE_INFINITY);
			setValue(mutableValue, query.QueryId);
		}

		double minDist = mutableValue.Dist;
		int minPre = mutableValue.Pre;
		for (SSSPMessageWritable msg : messages) {
			if (msg.Dist < minDist) {
				minDist = msg.Dist;
				minPre = msg.SrcVertex;
			}
		}

		if (minDist > query.MaxDist) {
			// Vertex is out of range
			setValue(null, query.QueryId);
			voteVertexHalt();
			return;
		}

		//System.out.println("COMP " + ID + " " + minDist);
		if (minDist < mutableValue.Dist) {
			mutableValue.Dist = minDist;
			mutableValue.Pre = minPre;
			for (Edge<DoubleWritable> edge : getEdges()) {
				sendMessageToVertex(new SSSPMessageWritable(ID, mutableValue.Dist + edge.Value.Value), edge.TargetVertexId);
			}
		}
		voteVertexHalt();
	}


	public static class Factory extends VertexFactory<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> {

		@Override
		public AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> newInstance(int id,
				VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> messageSender) {
			return new SSSPVertex(id, messageSender);
		}
	}
}
