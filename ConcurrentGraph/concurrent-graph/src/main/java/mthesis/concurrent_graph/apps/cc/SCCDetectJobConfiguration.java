package mthesis.concurrent_graph.apps.cc;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class SCCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, IntWritable, BaseQuery> {

	public SCCDetectJobConfiguration() {
		super(new SCCDetectVertex.Factory(), new IntWritable.Factory(), null, new IntWritable.Factory(),
				new BaseQuery.Factory());
	}
}
