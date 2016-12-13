package mthesis.concurrent_graph.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import mthesis.concurrent_graph.master.AbstractMasterInputReader;

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
public class EdgeListReader extends AbstractMasterInputReader {

	private final Random random = new Random(0);
	private final List<Integer> edges = new ArrayList<>();
	private int currentVertex;
	private int vertexCount = 0;
	private int edgeCount = 0;


	@Override
	public List<String> readAndPartition(String inputData, String inputDir, int numPartitions) {

		final List<String> partitions = new ArrayList<>(numPartitions);
		final List<PrintWriter> partitionWriters = new ArrayList<>(numPartitions);

		try (BufferedReader br = new BufferedReader(new FileReader(inputData))) {
			// Open partition writers
			for(int i = 0; i < numPartitions; i++) {
				final String fileName = inputDir + File.separator + i + ".txt";
				partitions.add(fileName);
				try {
					partitionWriters.add(new PrintWriter(new FileWriter(fileName)));
				}
				catch (final IOException e) {
					logger.error("opening files failed", e);
					return partitions;
				}
			}

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
					writeVertex(partitionWriters);
					edges.clear();
					currentVertex = vertex;
				}
				final int edge = Integer.parseInt(lineSplit[1]);
				vertices.add(edge);
				edges.add(edge);
			}
			writeVertex(partitionWriters);
			edges.clear();

			// Write vertices without outgoing edges
			vertices.removeAll(verticesWritten);
			for (final Integer vertex : vertices) {
				currentVertex = vertex;
				writeVertex(partitionWriters);
			}
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
		finally {
			for(final PrintWriter writer : partitionWriters) {
				writer.close();
			}
			logger.info("Loaded " + vertexCount + " vertices " + edgeCount + " edges");
		}

		return partitions;
	}

	private void writeVertex(List<PrintWriter> partitionWriters) {
		final PrintWriter writer = partitionWriters.get(random.nextInt(partitionWriters.size()));
		writer.println(currentVertex);
		for(final Integer edge : edges) {
			writer.println("\t" + edge);
		}
		vertexCount++;
		edgeCount += edges.size();
	}
}
