package mthesis.input_converter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.input_converter.VertexFileWriter.Edge;

/**
 * Converts from format [vertex] \t[edge] \t[edge] [vertex] ...
 * 
 * to standard vertex format
 * 
 * @author Jonas Grunert
 *
 */
public class VertexEdgesConverter {

	protected static final Logger logger = LoggerFactory.getLogger(VertexEdgesConverter.class);

	private static final String inputFile = "../../Data_original/cctest.txt";
	private static final String outputFile = "../../Data_converted/cctest.txt";



	public static void main(String[] args) throws Exception {

		final VertexFileWriter outWriter = new VertexFileWriter(outputFile);
		final List<Edge> edges = new ArrayList<>();
		Integer currentVertex;

		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			// Read and partition input
			String line;
			if ((line = br.readLine()) != null) currentVertex = Integer.parseInt(line);
			else return;

			while ((line = br.readLine()) != null) {
				if (line.startsWith("\t")) {
					edges.add(new Edge(Integer.parseInt(line.substring(1)), ""));
				}
				else {
					outWriter.writeVertex(currentVertex, "", edges);
					edges.clear();
					currentVertex = Integer.parseInt(line);
				}
			}
			outWriter.writeVertex(currentVertex, "", edges);
		}
		catch (final Exception e) {
			logger.error("loadVertices failed", e);
		}
		finally {
			outWriter.close();
		}


		logger.info("Converted and written to " + outputFile);
	}



	public static class Vertex {

		public final int Id;
		public final String Value;
		public final List<Edge> edges = new ArrayList<>();

		public Vertex(int id, String value) {
			super();
			Id = id;
			Value = value;
		}
	}
}
