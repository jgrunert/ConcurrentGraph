package mthesis.concurrent_graph.examples.pagerank;

import java.util.List;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.vertex.VertexMessage;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex for pagerank
 * 
 * @author Jonas Grunert
 *
 */
public class PagerankVertex extends AbstractVertex<DoubleWritable, NullWritable, DoubleWritable> {

	public PagerankVertex(int id, VertexWorkerInterface<DoubleWritable> messageSender) {
		super(id, messageSender);
	}

	@Override
	protected void compute(List<VertexMessage<DoubleWritable>> messages) {

	}


	public static class Factory extends VertexFactory<DoubleWritable, NullWritable, DoubleWritable> {

		@Override
		public AbstractVertex<DoubleWritable, NullWritable, DoubleWritable> newInstance(int id,
				VertexWorkerInterface<DoubleWritable> messageSender) {
			return new PagerankVertex(id, messageSender);
		}
	}
}
