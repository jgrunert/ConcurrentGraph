package mthesis.concurrent_graph.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.master.input.VertexPartitionMasterInputReader;

/**
 * Reads a file with an ordered list of edges. Format:
 * [vertex0]\t[edge0]
 * [vertex0]\t[edge1]
 * [vertex1]\t[edge0]
 * ...
 * 
 * @author Jonas Grunert
 *
 */
public class EdgeListReader extends VertexPartitionMasterInputReader {

	private final List<Integer> edges = new ArrayList<>();
	private int currentVertex;


	public EdgeListReader(int partitionSize, String partitionOutputDir) {
		super(partitionSize, partitionOutputDir);
	}


	@Override
	public void readAndPartition(String inputFile) {

		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {

			String line;
			while ((line = br.readLine()) != null && line.startsWith("#")) { }

			final Set<Integer> vertices = new HashSet<>();
			final Set<Integer> verticesWritten = new HashSet<>();

			// Read and partition input
			String[] lineSplit;
			if(line != null) {
				lineSplit = line.split("\t");
				currentVertex = Integer.parseInt(lineSplit[0]);
				final int edge = Integer.parseInt(lineSplit[1]);
				edges.add(edge);
				vertices.add(currentVertex);
				verticesWritten.add(currentVertex);
				vertices.add(edge);
			}

			// Read and partition vertices
			while ((line = br.readLine()) != null) {
				lineSplit = line.split("\t");
				final int vertex = Integer.parseInt(lineSplit[0]);
				if(vertex != currentVertex) {
					vertices.add(currentVertex);
					verticesWritten.add(currentVertex);
					writeVertex(currentVertex, edges);
					edges.clear();
					currentVertex = vertex;
				}
				final int edge = Integer.parseInt(lineSplit[1]);
				vertices.add(edge);
				edges.add(edge);
			}
			writeVertex(currentVertex, edges);
			edges.clear();

			// Write vertices without outgoing edges
			vertices.removeAll(verticesWritten);
			for (final Integer vertex : vertices) {
				currentVertex = vertex;
				writeVertex(currentVertex, edges);
			}
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
	}
}
