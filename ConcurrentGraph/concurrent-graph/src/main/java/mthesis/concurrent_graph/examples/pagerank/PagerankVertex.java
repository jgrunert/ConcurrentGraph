package mthesis.concurrent_graph.examples.pagerank;

import java.util.List;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
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
	protected void compute(List<DoubleWritable> messages) {
		if (superstepNo == 0) {
			setValue(new DoubleWritable(1.0 / getGlobalObjects().getVertexCount().Value));
		}
		else {
			double sum = 0;
			for (final DoubleWritable msg : messages) {
				sum += msg.Value;
			}
			final double value = 0.15 / getGlobalObjects().getVertexCount().Value + 0.85 * sum;
			// if(Math.abs(value - getValue().Value) < 0.000001)
			// voteHalt();
			getValue().Value = value;
		}

		if (superstepNo < 30) {
			final double n = getValue().Value / getEdges().size();
			sendMessageToAllOutgoingEdges(new DoubleWritable(n));
		}
		else {
			voteHalt();
		}
	}


	public static class Factory extends VertexFactory<DoubleWritable, NullWritable, DoubleWritable> {

		@Override
		public AbstractVertex<DoubleWritable, NullWritable, DoubleWritable> newInstance(int id,
				VertexWorkerInterface<DoubleWritable> messageSender) {
			return new PagerankVertex(id, messageSender);
		}
	}
}
