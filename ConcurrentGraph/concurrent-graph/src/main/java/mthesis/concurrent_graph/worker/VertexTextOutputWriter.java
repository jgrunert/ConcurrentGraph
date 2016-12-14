package mthesis.concurrent_graph.worker;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

/**
 * Writes text output file in format
 * [vertex0ID]\t[vertex0Value]
 * [vertex1ID]...
 * 
 * @author Jonas Grunert
 */
public class VertexTextOutputWriter<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> {
	private  final Logger logger = LoggerFactory.getLogger(this.getClass());

	public void writeOutput(String file, List<AbstractVertex<V, E, M>> vertices) {
		try(PrintWriter writer = new PrintWriter(new FileWriter(file)))
		{
			for(final AbstractVertex<V, E, M> vertex : vertices) {
				final V value = vertex.getValue();
				final String vertexValue = value != null ? value.GetString() : "";
				writer.println(vertex.ID + "\t" + vertexValue);
			}
		}
		catch(final Exception e)
		{
			logger.error("writeOutput failed", e);
		}
	}
}
