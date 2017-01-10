package mthesis.concurrent_graph.examples.cc;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.QueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, CCMessageWritable, QueryGlobalValues> {

	@Override
	public VertexFactory<IntWritable, NullWritable, CCMessageWritable, QueryGlobalValues> getVertexFactory() {
		return new CCDetectVertex.Factory();
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
	public BaseWritableFactory<CCMessageWritable> getMessageValueFactory() {
		return new CCMessageWritable.Factory();
	}

	@Override
	public BaseQueryGlobalValuesFactory<QueryGlobalValues> getGlobalValuesFactory() {
		return new QueryGlobalValues.Factory();
	}

}
