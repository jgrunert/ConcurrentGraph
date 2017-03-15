package mthesis.concurrent_graph.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.graph.Edge;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

/**
 * Reads input from file with standard format Vertex0,VertexValue0|Edge1Neighbor,Edge1Value;Edge2Neighbor,Edge2Value Vertex1...
 *
 * @author Jonas Grunert
 */
public class VertexTextInputReader<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues>
		implements BaseVertexInputReader<V, E, M, Q> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@SuppressWarnings("unchecked")
	@Override
	public List<AbstractVertex<V, E, M, Q>> getVertices(List<String> partitions, JobConfiguration<V, E, M, Q> jobConfig,
			VertexWorkerInterface<V, E, M, Q> vertexMessageSender) {

		final VertexFactory<V, E, M, Q> vertexFactory = jobConfig.getVertexFactory();
		final BaseWritableFactory<V> vertexValueFactory = jobConfig.getVertexValueFactory();
		final BaseWritableFactory<E> edgeValueFacgory = jobConfig.getEdgeValueFactory();

		final List<AbstractVertex<V, E, M, Q>> vertices = new ArrayList<>();

		for (final String partition : partitions) {
			try (BufferedReader br = new BufferedReader(new FileReader(partition))) {
				String line;

				while ((line = br.readLine()) != null) {
					final String[] split0 = line.split("\\|");

					// Vertex ID
					final String[] splitVertex = split0[0].split(",");
					final int vertexId = Integer.parseInt(splitVertex[0]);
					final AbstractVertex<V, E, M, Q> vertex = vertexFactory.newInstance(vertexId, vertexMessageSender);

					// Optional vertex value
					if (splitVertex.length > 1 && vertexValueFactory != null) {
						vertex.setDefaultValue(vertexValueFactory.createFromString(splitVertex[1]));
					}

					// Vertex edges
					final List<Edge<E>> edges = new ArrayList<>();
					if (split0.length > 1) {
						final String[] splitEdges = split0[1].split(";");
						for (final String edgeStr : splitEdges) {
							final String[] splitEdgeStr = edgeStr.split(",");
							// Optional edge value
							E edgeValue;
							if (splitEdgeStr.length > 1 && edgeValueFacgory != null) {
								edgeValue = edgeValueFacgory.createFromString(splitEdgeStr[1]);
							}
							else {
								edgeValue = null;
							}
							edges.add(new Edge<E>(Integer.parseInt(splitEdgeStr[0]), edgeValue));
						}
					}
					vertex.setEdges(edges.toArray((Edge<E>[]) new Object[0]));

					vertices.add(vertex);
				}
			}
			catch (final Exception e) {
				logger.error("loadVertices failed", e);
			}
		}

		return vertices;
	}
}
