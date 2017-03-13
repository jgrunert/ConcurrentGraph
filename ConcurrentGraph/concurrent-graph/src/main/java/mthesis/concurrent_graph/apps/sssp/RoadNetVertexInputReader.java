package mthesis.concurrent_graph.apps.sssp;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.BaseVertexInputReader;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.DoubleWritable;

/**
 * Reads input from file with standard format Vertex0,VertexValue0|Edge1Neighbor,Edge1Value;Edge2Neighbor,Edge2Value Vertex1...
 *
 * @author Jonas Grunert
 */
public class RoadNetVertexInputReader
		implements BaseVertexInputReader<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public List<AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues>> getVertices(
			List<String> partitions, JobConfiguration<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> jobConfig,
			VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> vertexWorkerInterface) {
		final List<AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues>> vertices = new ArrayList<>();

		for (final String partition : partitions) {
			try (DataInputStream reader = new DataInputStream(new BufferedInputStream(new FileInputStream(partition)))) {
				int numVertices = reader.readInt();

				for (int iV = 0; iV < numVertices; iV++) {
					int vertexId = reader.readInt();
					SSSPVertex vertex = new SSSPVertex(vertexId, vertexWorkerInterface);

					int edgeCount = reader.readInt();
					int[] edgeTargets = new int[edgeCount];
					DoubleWritable[] edgeValues = new DoubleWritable[edgeCount];
					for (int i = 0; i < edgeCount; i++) {
						edgeTargets[i] = reader.readInt();
						edgeValues[i] = new DoubleWritable(reader.readDouble());
					}
					vertex.setEdges(edgeTargets, edgeValues);

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
