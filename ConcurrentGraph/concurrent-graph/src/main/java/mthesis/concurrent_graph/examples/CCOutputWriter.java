package mthesis.concurrent_graph.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import mthesis.concurrent_graph.master.AbstractMasterOutputWriter;

public class CCOutputWriter extends AbstractMasterOutputWriter {

	@Override
	public void writeOutput(String outputDir) {
		// Aggregate output
		try(PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "combined.txt")))
		{
			final File outFolder = new File(outputDir);
			for(final File f : outFolder.listFiles()) {
				for(final String line : Files.readAllLines(Paths.get(f.getPath()), Charset.forName("UTF-8")) ){
					writer.println(line);
				}
			}
		}
		catch(final Exception e)
		{
			logger.error("writeOutput failed", e);
		}
	}
}
