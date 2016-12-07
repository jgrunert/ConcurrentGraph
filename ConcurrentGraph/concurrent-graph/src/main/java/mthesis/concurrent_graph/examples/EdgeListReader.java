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
	private final List<String> edges = new ArrayList<>();
	private String currentVertex;
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

			// Read and partition input
			String[] lineSplit;
			if(line != null) {
				lineSplit = line.split("\t");
				currentVertex = lineSplit[0];
				edges.add(lineSplit[1]);
			}

			while ((line = br.readLine()) != null) {
				lineSplit = line.split("\t");
				if(!lineSplit[0].equals(currentVertex)) {
					writeVertex(partitionWriters);
					edges.clear();
					currentVertex = lineSplit[0];
				}
				edges.add("\t" + lineSplit[1]);
			}
			writeVertex(partitionWriters);
			edges.clear();
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
		finally {
			for(final PrintWriter writer : partitionWriters) {
				writer.close();
			}
			System.out.println("Loaded " + vertexCount + " vertices " + edgeCount + " edges");
		}

		return partitions;
	}

	private void writeVertex(List<PrintWriter> partitionWriters) {
		final PrintWriter writer = partitionWriters.get(random.nextInt(partitionWriters.size()));
		writer.println(currentVertex);
		for(final String edge : edges) {
			writer.println(edge);
		}
		vertexCount++;
		edgeCount += edges.size();
	}
}
