package mthesis.concurrent_graph.apps.pagerank;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankJobConfiguration extends JobConfiguration<DoubleWritable, NullWritable, DoubleWritable, BaseQuery> {

	public PagerankJobConfiguration() {
		super(new PagerankVertex.Factory(), new DoubleWritable.Factory(), null, new DoubleWritable.Factory(),
				new BaseQuery.Factory());
	}
}
