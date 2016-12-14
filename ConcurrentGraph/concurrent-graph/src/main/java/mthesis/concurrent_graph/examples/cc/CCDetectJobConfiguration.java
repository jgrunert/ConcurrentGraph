package mthesis.concurrent_graph.examples.cc;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, IntWritable> {

	@Override
	public VertexFactory<IntWritable, NullWritable, IntWritable> getVertexFactory() {
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
	public BaseWritableFactory<IntWritable> getMessageValueFactory() {
		return new IntWritable.Factory();
	}

}
