package mthesis.input_converter;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertexFileWriter {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final PrintWriter writer;
	private final StringBuilder stringBuilder = new StringBuilder();


	public VertexFileWriter(String outFile) throws FileNotFoundException {
		writer = new PrintWriter(outFile);
	}

	public void close() {
		writer.close();
	}

	public void writeVertex(int vertexId, String vertexValue, List<Edge> edges) {
		stringBuilder.append(vertexId);
		stringBuilder.append(',');
		stringBuilder.append(vertexValue);
		stringBuilder.append('|');
		for (final Edge edge : edges) {
			stringBuilder.append(edge.NeighborId);
			stringBuilder.append(',');
			stringBuilder.append(edge.Value);
			stringBuilder.append(';');
		}
		writer.println(stringBuilder.toString());
		stringBuilder.setLength(0);
	}


	public static class Edge {

		public final int NeighborId;
		public final String Value;

		public Edge(int neighborId, String value) {
			super();
			NeighborId = neighborId;
			Value = value;
		}
	}
}
