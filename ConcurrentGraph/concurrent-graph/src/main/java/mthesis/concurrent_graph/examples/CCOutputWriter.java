package mthesis.concurrent_graph.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mthesis.concurrent_graph.master.AbstractMasterOutputWriter;

public class CCOutputWriter extends AbstractMasterOutputWriter {

	@Override
	public void writeOutput(String outputDir) {
		// Connectec components ID->VertexCount
		final Map<Integer, Integer> components = new HashMap<>();

		// Aggregate output
		try(PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "combined.txt")))
		{
			final File outFolder = new File(outputDir);
			for(final File f : outFolder.listFiles()) {
				for(final String line : Files.readAllLines(Paths.get(f.getPath()), Charset.forName("UTF-8")) ){
					writer.println(line);
					final int comp = Integer.parseInt(line.split("\t")[1]);
					final Integer compCounter = components.get(comp);
					components.put(comp, compCounter != null ? compCounter + 1 : 1);
				}
			}
		}
		catch(final Exception e)
		{
			logger.error("writeOutput failed", e);
		}

		try(PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "components.txt")))
		{
			for(final Entry<Integer, Integer> comp : components.entrySet()) {
				writer.println(comp.getKey() + "\t" + comp.getValue());
			}
		}
		catch(final Exception e)
		{
			logger.error("writeOutput failed", e);
		}
	}
}
