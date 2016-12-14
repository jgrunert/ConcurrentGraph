package mthesis.input_converter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.input_converter.VertexFileWriter.Edge;

/**
 * Reads a file with an ordered list of edges. Format: [vertex0]\t[edge0]
 * [vertex0]\t[edge1] [vertex1]\t[edge0] ...
 * 
 * Converts to standard vertex format
 * 
 * @author Jonas Grunert
 *
 */
public class EdgeListConverter {

	protected static final Logger logger = LoggerFactory.getLogger(EdgeListConverter.class);

	private static final String inputFile = "../../Data_original/Wiki-Vote.txt";
	private static final String outputFile = "../../Data_converted/Wiki-Vote.txt";


	public static void main(String[] args) throws Exception {

		final VertexFileWriter outWriter = new VertexFileWriter(outputFile);

		final List<Integer> verticesList = new ArrayList<>();
		final Map<Integer, List<Edge>> verticesMap = new HashMap<>();


		try (BufferedReader inReader = new BufferedReader(new FileReader(inputFile))) {
			// Skip header
			String line;
			while ((line = inReader.readLine()) != null && line.startsWith("#")) {
			}

			String[] lineSplit;
			while ((line = inReader.readLine()) != null) {
				lineSplit = line.split("\t");
				final int v0 = Integer.parseInt(lineSplit[0]);
				final int v1 = Integer.parseInt(lineSplit[1]);

				List<Edge> vertexEdges = verticesMap.get(v0);
				if (vertexEdges == null) {
					vertexEdges = new ArrayList<>();
					verticesList.add(v0);
					verticesMap.put(v0, vertexEdges);
				}
				vertexEdges.add(new Edge(Integer.parseInt(lineSplit[1]), ""));

				if (!verticesMap.containsKey(v1)) {
					verticesList.add(v1);
					verticesMap.put(v1, new ArrayList<>());
				}
			}

			for (final Integer vertex : verticesList) {
				outWriter.writeVertex(vertex, "", verticesMap.get(vertex));
			}
		}
		catch (final Exception e) {
			logger.error("count vertices failed", e);
			return;
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
