package mthesis.concurrent_graph.examples.cc;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class SCCDetectJobConfiguration extends JobConfiguration<IntWritable, NullWritable, IntWritable, BaseQueryGlobalValues> {

	public SCCDetectJobConfiguration() {
		super(new SCCDetectVertex.Factory(), new IntWritable.Factory(), null, new IntWritable.Factory(),
				new BaseQueryGlobalValues.Factory());
	}
}
