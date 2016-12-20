package mthesis.concurrent_graph.examples.sssp;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPJobConfiguration extends JobConfiguration<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable> {

	@Override
	public VertexFactory<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable> getVertexFactory() {
		return new SSSPVertex.Factory();
	}

	@Override
	public BaseWritableFactory<SSSPVertexWritable> getVertexValueFactory() {
		return new SSSPVertexWritable.Factory();
	}

	@Override
	public BaseWritableFactory<DoubleWritable> getEdgeValueFactory() {
		return new DoubleWritable.Factory();
	}

	@Override
	public BaseWritableFactory<SSSPMessageWritable> getMessageValueFactory() {
		return new SSSPMessageWritable.Factory();
	}

}
