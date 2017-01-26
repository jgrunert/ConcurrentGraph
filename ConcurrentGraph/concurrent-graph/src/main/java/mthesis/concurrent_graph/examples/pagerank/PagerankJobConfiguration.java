package mthesis.concurrent_graph.examples.pagerank;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankJobConfiguration extends JobConfiguration<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> {

	public PagerankJobConfiguration() {
		super(new PagerankVertex.Factory(), new DoubleWritable.Factory(), null, new DoubleWritable.Factory(),
				new BaseQueryGlobalValues.Factory());
	}
}
