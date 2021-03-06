package mthesis.concurrent_graph.apps.cc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;

public class CCOutputWriter extends MasterOutputEvaluator<BaseQuery> {

	public CCOutputWriter() {
		super();
	}

	@Override
	public void evaluateOutput(String outputDir, BaseQuery query) {
		// Connectec components ID->VertexCount
		final Map<Integer, Integer> components = new HashMap<>();

		// Aggregate output
		final File outFolder = new File(outputDir);
		final File[] outFiles = outFolder.listFiles();
		try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "combined.txt"))) {
			for (final File f : outFiles) {
				try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
					String line;
					while ((line = reader.readLine()) != null) {
						writer.println(line);
						final int comp = Integer.parseInt(line.split("\t")[1]);
						final Integer compCounter = components.get(comp);
						components.put(comp, compCounter != null ? compCounter + 1 : 1);
					}
				}
			}
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}

		try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "components.txt"))) {
			for (final Entry<Integer, Integer> comp : components.entrySet()) {
				writer.println(comp.getKey() + "\t" + comp.getValue());
			}
		}
		catch (final Exception e) {
			logger.error("writeOutput failed", e);
		}
	}
}
