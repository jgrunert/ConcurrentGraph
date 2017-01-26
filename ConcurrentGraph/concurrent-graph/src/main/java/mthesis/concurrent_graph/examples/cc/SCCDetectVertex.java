package mthesis.concurrent_graph.examples.cc;

import java.nio.ByteBuffer;
import java.util.List;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.worker.WorkerQuery;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

/**
 * Example vertex to detect strongly connected components in a graph
 *
 * @author Jonas Grunert
 *
 */
public class SCCDetectVertex extends AbstractVertex<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> {

	public SCCDetectVertex(int id,
			VertexWorkerInterface<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> messageSender) {
		super(id, messageSender);
	}

	public SCCDetectVertex(ByteBuffer bufferToRead,
			VertexWorkerInterface<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> worker,
			JobConfiguration<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> jobConfig) {
		super(bufferToRead, worker, jobConfig);
	}

	@Override
	protected void compute(int superstepNo, List<IntWritable> messages,
			WorkerQuery<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> query) {
		if (superstepNo == 0) {
			IntWritable value = new IntWritable(ID);
			setValue(value, query.QueryId);
			sendMessageToAllOutgoingEdges(value, query);
			voteVertexHalt(query.QueryId);
			return;
		}

		IntWritable mutableValue = getValue(query.QueryId);
		int min = mutableValue.Value;
		for (final IntWritable msg : messages) {
			final int msgValue = msg.Value;
			//System.out.println("Get " + msgValue + " on " + ID + " from " + msg.SrcVertex);
			min = Math.min(min, msgValue);
		}

		if (min < mutableValue.Value) {
			mutableValue.Value = min;
			sendMessageToAllOutgoingEdges(mutableValue, query);
		}
		else {
			//System.out.println("Vote halt on " + ID + " with " + value);
			voteVertexHalt(query.QueryId);
		}
	}


	public static class Factory extends VertexFactory<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> {

		@Override
		public AbstractVertex<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> newInstance(int id,
				VertexWorkerInterface<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> messageSender) {
			return new SCCDetectVertex(id, messageSender);
		}

		@Override
		public AbstractVertex<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> newInstance(ByteBuffer bufferToRead,
				VertexWorkerInterface<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> worker,
				JobConfiguration<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> jobConfig) {
			return new SCCDetectVertex(bufferToRead, worker, jobConfig);
		}
	}
}
