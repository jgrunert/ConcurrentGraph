package mthesis.concurrent_graph.worker;

import java.util.List;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

/**
 * Base class for reading vertex input
 *
 * @author Jonas Grunert
 */
public interface BaseVertexInputReader<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, G extends BaseQuery> {

	public List<AbstractVertex<V, E, M, G>> getVertices(List<String> partitions, JobConfiguration<V, E, M, G> jobConfig,
			VertexWorkerInterface<V, E, M, G> vertexMessageSender);
}
