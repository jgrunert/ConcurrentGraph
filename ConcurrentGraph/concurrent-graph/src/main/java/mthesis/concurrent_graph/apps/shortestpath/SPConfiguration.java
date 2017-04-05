package mthesis.concurrent_graph.apps.shortestpath;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SPConfiguration extends JobConfiguration<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> {

	public SPConfiguration() {
		super(new SPVertex.Factory(), new SPVertexWritable.Factory(), new DoubleWritable.Factory(), new SPMessageWritable.Factory(),
				new SPQuery.Factory());
	}
}
