package mthesis.concurrent_graph.apps.pagerank;

import java.nio.ByteBuffer;
import java.util.List;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.worker.WorkerQuery;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex for pagerank
 *
 * @author Jonas Grunert
 *
 */
public class PagerankVertex extends AbstractVertex<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> {

	public PagerankVertex(int id,
			VertexWorkerInterface<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> messageSender) {
		super(id, messageSender);
	}

	public PagerankVertex(ByteBuffer bufferToRead,
			VertexWorkerInterface<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> worker,
			JobConfiguration<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> jobConfig) {
		super(bufferToRead, worker, jobConfig);
	}


	@Override
	protected void compute(int superstepNo, List<DoubleWritable> messages,
			WorkerQuery<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> query) {
		DoubleWritable mutableValue;
		if (superstepNo == 0) {
			mutableValue = new DoubleWritable(1.0 / query.Query.getVertexCount());
			setValue(mutableValue, query.QueryId);
		}
		else {
			double sum = 0;
			for (final DoubleWritable msg : messages) {
				sum += msg.Value;
			}
			final double value = 0.15 / query.Query.getVertexCount() + 0.85 * sum;
			mutableValue = getValue(query.QueryId);
			if (Math.abs(value - mutableValue.Value) < 0.000001) voteVertexHalt(query.QueryId);
			mutableValue.Value = value;
		}

		if (superstepNo < 30) {
			final double n = mutableValue.Value / edgeTargets.length;
			sendMessageToAllOutgoingEdges(new DoubleWritable(n), query);
		}
		else {
			voteVertexHalt(query.QueryId);
		}
	}


	public static class Factory extends VertexFactory<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> {

		@Override
		public AbstractVertex<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> newInstance(int id,
				VertexWorkerInterface<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> messageSender) {
			return new PagerankVertex(id, messageSender);
		}

		@Override
		public AbstractVertex<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> newInstance(ByteBuffer bufferToRead,
				VertexWorkerInterface<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> worker,
				JobConfiguration<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> jobConfig) {
			return new PagerankVertex(bufferToRead, worker, jobConfig);
		}
	}
}
