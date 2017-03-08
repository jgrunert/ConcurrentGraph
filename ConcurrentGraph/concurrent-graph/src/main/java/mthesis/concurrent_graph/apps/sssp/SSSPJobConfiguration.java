package mthesis.concurrent_graph.apps.sssp;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPJobConfiguration extends JobConfiguration<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> {

	public SSSPJobConfiguration() {
		super(new SSSPVertex.Factory(), new SSSPVertexWritable.Factory(), new DoubleWritable.Factory(), new SSSPMessageWritable.Factory(),
				new SSSPQueryValues.Factory());
	}
}
