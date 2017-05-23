package mthesis.concurrent_graph.apps.shortestpath.partitioning;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.apps.shortestpath.SPMessageWritable;
import mthesis.concurrent_graph.apps.shortestpath.SPQuery;
import mthesis.concurrent_graph.apps.shortestpath.SPVertex;
import mthesis.concurrent_graph.apps.shortestpath.SPVertexWritable;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.Edge;
import mthesis.concurrent_graph.worker.BaseVertexInputReader;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.DoubleWritable;

/**
 * Used by workers to read input partitions
 * Reads input from file with standard format Vertex0,VertexValue0|Edge1Neighbor,Edge1Value;Edge2Neighbor,Edge2Value Vertex1...
 *
 * @author Jonas Grunert
 */
public class RoadNetWorkerPartitionReader
		implements BaseVertexInputReader<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final boolean SearchNextTagTestMode = Configuration.getPropertyBoolDefault("SearchNextTagTestMode", false);
	private static final double SearchNextTagProbability = Configuration.getPropertyDoubleDefault("SearchNextTagProbability", 0.001);
	private static final int SearchNextTagNumTags = Configuration.getPropertyIntDefault("SearchNextTagNumTags", 100);


	@Override
	public List<AbstractVertex<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery>> getVertices(
			List<String> partitions, JobConfiguration<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> jobConfig,
			VertexWorkerInterface<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> vertexWorkerInterface) {
		final List<AbstractVertex<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery>> vertices = new ArrayList<>();

		Random tagRandom = new Random(0);

		for (final String partition : partitions) {
			try (DataInputStream reader = new DataInputStream(new BufferedInputStream(new FileInputStream(partition)))) {
				int numVertices = reader.readInt();

				for (int iV = 0; iV < numVertices; iV++) {
					int vertexId = reader.readInt();

					int tag;
					if (SearchNextTagTestMode) {
						if (tagRandom.nextDouble() < SearchNextTagProbability) {
							tag = tagRandom.nextInt(SearchNextTagNumTags);
						}
						else {
							tag = -1;
						}
					}
					else {
						tag = -1;
					}

					SPVertex vertex = new SPVertex(vertexId, tag, vertexWorkerInterface);

					int numEdges = reader.readInt();
					final List<Edge<DoubleWritable>> edges = new ArrayList<>(numEdges);
					for (int iEdge = 0; iEdge < numEdges; iEdge++) {
						edges.add(new Edge<DoubleWritable>(reader.readInt(), new DoubleWritable(reader.readDouble())));
					}
					vertex.setEdges(edges);
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
