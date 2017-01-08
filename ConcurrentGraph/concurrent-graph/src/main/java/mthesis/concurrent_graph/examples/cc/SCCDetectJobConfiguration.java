package mthesis.concurrent_graph.examples.cc;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class SCCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, IntWritable, QueryGlobalValues> {

	@Override
	public VertexFactory<IntWritable, NullWritable, IntWritable, QueryGlobalValues> getVertexFactory() {
		return new SCCDetectVertex.Factory();
	}

	@Override
	public BaseWritableFactory<IntWritable> getVertexValueFactory() {
		return new IntWritable.Factory();
	}

	@Override
	public BaseWritableFactory<NullWritable> getEdgeValueFactory() {
		return null;
	}

	@Override
	public BaseWritableFactory<IntWritable> getMessageValueFactory() {
		return new IntWritable.Factory();
	}

	@Override
	public BaseWritableFactory<QueryGlobalValues> getGlobalValuesFactory() {
		return new QueryGlobalValues.Factory();
	}

}
