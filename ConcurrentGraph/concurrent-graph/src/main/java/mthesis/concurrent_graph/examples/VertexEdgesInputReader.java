package mthesis.concurrent_graph.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mthesis.concurrent_graph.master.AbstractMasterInputReader;

/**
 * Reads a file with format
 * [vertex]
 * \t[edge]
 * \t[edge]
 * [vertex]
 * ...
 * 
 * @author Jonas Grunert
 *
 */
public class VertexEdgesInputReader extends AbstractMasterInputReader {

	private final Random random = new Random(0);
	private final List<String> edges = new ArrayList<>();
	private String currentVertex;


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

			// Read and partition input
			String line;
			if((line = br.readLine()) != null)
				currentVertex = line;

			while ((line = br.readLine()) != null) {
				if (line.startsWith("\t")) {
					edges.add(line);
				} else {
					writeVertex(partitionWriters);
					edges.clear();
					currentVertex = line;
				}
			}
			writeVertex(partitionWriters);
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
		finally {
			for(final PrintWriter writer : partitionWriters) {
				writer.close();
			}
		}

		return partitions;
	}

	private void writeVertex(List<PrintWriter> partitionWriters) {
		final PrintWriter writer = partitionWriters.get(random.nextInt(partitionWriters.size()));
		writer.println(currentVertex);
		for(final String edge : edges) {
			writer.println(edge);
		}
	}
}
