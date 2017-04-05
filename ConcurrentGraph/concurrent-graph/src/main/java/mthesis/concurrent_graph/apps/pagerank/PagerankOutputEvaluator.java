package mthesis.concurrent_graph.apps.pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;

public class PagerankOutputEvaluator extends MasterOutputEvaluator<BaseQuery> {

	public PagerankOutputEvaluator() {
		super();
	}

	@Override
	public void evaluateOutput(String outputDir, BaseQuery query) {
		// Aggregate output
		final File outFolder = new File(outputDir);
		final File[] outFiles = outFolder.listFiles();
		try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "combined.txt"))) {
			for (final File f : outFiles) {
				try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
					String line;
					while ((line = reader.readLine()) != null) {
						writer.println(line);
					}
				}
			}
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}
	}
}
