package mthesis.input_converter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.input_converter.VertexFileWriter.Edge;

public class EdgeListConverter {

	protected static final Logger logger = LoggerFactory.getLogger(EdgeListConverter.class);

	private static final String inputFile = "../../Data_original/Wiki-Vote.txt";
	private static final String outputFile = "../../Data_converted/Wiki-Vote.txt";

	private static final List<Edge> edges = new ArrayList<>();
	private static int currentVertex;

	private static final Set<Integer> vertices = new HashSet<>();
	private static final Set<Integer> verticesWritten = new HashSet<>();


	public static void main(String[] args) throws Exception {

		final VertexFileWriter outWriter = new VertexFileWriter(outputFile);
		try (BufferedReader inReader = new BufferedReader(new FileReader(inputFile))) {

			String line;
			while ((line = inReader.readLine()) != null && line.startsWith("#")) {
			}

			final Set<Integer> vertices = new HashSet<>();
			final Set<Integer> verticesWritten = new HashSet<>();

			// Read and partition input
			String[] lineSplit;
			if (line != null) {
				lineSplit = line.split("\t");
				currentVertex = Integer.parseInt(lineSplit[0]);
				final int edge = Integer.parseInt(lineSplit[1]);
				edges.add(new Edge(edge, ""));
				vertices.add(currentVertex);
				verticesWritten.add(currentVertex);
				vertices.add(edge);
			}

			// Read and partition vertices
			while ((line = inReader.readLine()) != null) {
				lineSplit = line.split("\t");
				final int vertex = Integer.parseInt(lineSplit[0]);
				if (vertex != currentVertex) {
					vertices.add(currentVertex);
					verticesWritten.add(currentVertex);
					outWriter.writeVertex(currentVertex, "", edges);
					edges.clear();
					currentVertex = vertex;
				}
				final int edge = Integer.parseInt(lineSplit[1]);
				vertices.add(edge);
				edges.add(new Edge(edge, ""));
			}
			outWriter.writeVertex(currentVertex, "", edges);
			edges.clear();

			// Write vertices without outgoing edges
			vertices.removeAll(verticesWritten);
			for (final Integer vertex : vertices) {
				currentVertex = vertex;
				outWriter.writeVertex(currentVertex, "", edges);
			}
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
		outWriter.close();

		logger.info("Converted and written to " + outputFile);
	}
}
