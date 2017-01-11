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
public class SSSPVertex extends AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPGlobalValues> {

	public SSSPVertex(int id, VertexWorkerInterface<SSSPMessageWritable, SSSPGlobalValues> messageSender) {
		super(id, messageSender);
		setValue(null); // Never activated nodes don't have a value
	}

	@Override
	protected void compute(List<SSSPMessageWritable> messages) {
		if (superstepNo == 0) {
			if (ID != getGlobalQueryValues().From) {
				voteVertexInactive();
				return;
			}
			else {
				System.out.println("GO " + ID);
				SSSPVertexWritable mutableValue = new SSSPVertexWritable(-1, 0);
				setValue(mutableValue);
				for (Edge<DoubleWritable> edge : getEdges()) {
					sendMessageToVertex(new SSSPMessageWritable(ID, edge.Value.Value), edge.TargetVertexId);
				}
				return;
			}
		}

		SSSPVertexWritable mutableValue = getValue();
		if (mutableValue == null) {
			mutableValue = new SSSPVertexWritable(-1, Double.POSITIVE_INFINITY);
			setValue(mutableValue);
		}

		double minDist = mutableValue.Dist;
		int minPre = mutableValue.Pre;
		for (SSSPMessageWritable msg : messages) {
			if (msg.Dist < minDist) {
				minDist = msg.Dist;
				minPre = msg.SrcVertex;
			}
		}

		if (minDist > getGlobalQueryValues().MaxDist) {
			// Vertex is out of range
			setValue(null);
			voteVertexInactive();
			return;
		}

		System.out.println("COMP " + ID + " " + minDist);
		if (minDist < mutableValue.Dist) {
			mutableValue.Dist = minDist;
			mutableValue.Pre = minPre;
			for (Edge<DoubleWritable> edge : getEdges()) {
				sendMessageToVertex(new SSSPMessageWritable(ID, mutableValue.Dist + edge.Value.Value), edge.TargetVertexId);
			}
		}
		voteVertexInactive();
	}


	public static class Factory extends VertexFactory<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPGlobalValues> {

		@Override
		public AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPGlobalValues> newInstance(int id,
				VertexWorkerInterface<SSSPMessageWritable, SSSPGlobalValues> messageSender) {
			return new SSSPVertex(id, messageSender);
		}
	}
}
