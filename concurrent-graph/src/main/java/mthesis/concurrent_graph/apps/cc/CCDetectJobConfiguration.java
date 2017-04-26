package mthesis.concurrent_graph.apps.cc;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, CCMessageWritable, BaseQuery> {

	public CCDetectJobConfiguration() {
		super(new CCDetectVertex.Factory(), new IntWritable.Factory(), null, new CCMessageWritable.Factory(),
				new BaseQuery.Factory());
	}
}
