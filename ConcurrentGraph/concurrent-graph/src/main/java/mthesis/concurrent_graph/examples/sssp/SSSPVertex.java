package mthesis.concurrent_graph.examples.sssp;

import java.util.List;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.DoubleWritable;

/**
 * Example vertex for single source shortest path
 * 
 * @author Jonas Grunert
 *
 */
public class SSSPVertex extends AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable> {

	public SSSPVertex(int id, VertexWorkerInterface<SSSPMessageWritable> messageSender) {
		super(id, messageSender);
		setValue(new SSSPVertexWritable(-1, Double.POSITIVE_INFINITY));
	}

	@Override
	protected void compute(List<SSSPMessageWritable> messages) {
		voteHalt();
	}


	public static class Factory extends VertexFactory<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable> {

		@Override
		public AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable> newInstance(int id,
				VertexWorkerInterface<SSSPMessageWritable> messageSender) {
			return new SSSPVertex(id, messageSender);
		}
	}
}
