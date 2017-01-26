package mthesis.concurrent_graph.examples.cc;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, CCMessageWritable, BaseQueryGlobalValues> {

	public CCDetectJobConfiguration() {
		super(new CCDetectVertex.Factory(), new IntWritable.Factory(), null, new CCMessageWritable.Factory(),
				new BaseQueryGlobalValues.Factory());
	}
}
