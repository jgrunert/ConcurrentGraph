package mthesis.concurrent_graph.examples.pagerank;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankJobConfiguration extends JobConfiguration<DoubleWritable, NullWritable, DoubleWritable> {

	@Override
	public VertexFactory<DoubleWritable, NullWritable, DoubleWritable> getVertexFactory() {
		return new PagerankVertex.Factory();
	}

	@Override
	public BaseWritableFactory<DoubleWritable> getVertexValueFactory() {
		return new DoubleWritable.Factory();
	}

	@Override
	public BaseWritableFactory<NullWritable> getEdgeValueFactory() {
		return null;
	}

	@Override
	public BaseWritableFactory<DoubleWritable> getMessageValueFactory() {
		return new DoubleWritable.Factory();
	}

}