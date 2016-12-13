package mthesis.concurrent_graph.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.master.input.VertexPartitionMasterInputReader;

/**
 * Reads and partitions a file with format
 * [vertex]
 * \t[edge]
 * \t[edge]
 * [vertex]
 * ...
 * 
 * @author Jonas Grunert
 *
 */
public class VertexEdgesInputReader extends VertexPartitionMasterInputReader {
	private final List<Integer> edges = new ArrayList<>();
	private Integer currentVertex;


	protected VertexEdgesInputReader(int partitionSize, String partitionOutputDir) {
		super(partitionSize, partitionOutputDir);
	}


	@Override
	public void readAndPartition(String inputFile) {
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			// Read and partition input
			String line;
			if((line = br.readLine()) != null)
				currentVertex = Integer.parseInt(line);

			while ((line = br.readLine()) != null) {
				if (line.startsWith("\t")) {
					edges.add(Integer.parseInt(line));
				} else {
					writeVertex(currentVertex, edges);
					edges.clear();
					currentVertex = Integer.parseInt(line);
				}
			}
			writeVertex(currentVertex, edges);
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
	}
}
