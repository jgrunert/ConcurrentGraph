package mthesis.concurrent_graph.examples.cc;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, CCMessageWritable, BaseQueryGlobalValues> {

	private final CCDetectVertex.Factory vertexFactory = new CCDetectVertex.Factory(this);
	private final BaseWritableFactory<IntWritable> vertexValueFactory = new IntWritable.Factory();
	private final BaseWritableFactory<CCMessageWritable> messageValueFactory = new CCMessageWritable.Factory();
	private final BaseQueryGlobalValuesFactory<BaseQueryGlobalValues> globalValueFactory = new BaseQueryGlobalValues.Factory();

	@Override
	public VertexFactory<IntWritable, NullWritable, CCMessageWritable, BaseQueryGlobalValues> getVertexFactory() {
		return vertexFactory;
	}

	@Override
	public BaseWritableFactory<IntWritable> getVertexValueFactory() {
		return vertexValueFactory;
	}

	@Override
	public BaseWritableFactory<NullWritable> getEdgeValueFactory() {
		return null;
	}

	@Override
	public BaseWritableFactory<CCMessageWritable> getMessageValueFactory() {
		return messageValueFactory;
	}

	@Override
	public BaseQueryGlobalValuesFactory<BaseQueryGlobalValues> getGlobalValuesFactory() {
		return globalValueFactory;
	}

}
