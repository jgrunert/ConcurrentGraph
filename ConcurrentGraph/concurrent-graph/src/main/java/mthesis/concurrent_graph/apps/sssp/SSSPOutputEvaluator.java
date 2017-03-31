package mthesis.concurrent_graph.apps.sssp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.util.Pair;

public class SSSPOutputEvaluator extends MasterOutputEvaluator<SSSPQueryValues> {

	public SSSPOutputEvaluator() {
		super();
	}

	@Override
	public void evaluateOutput(String outputDir, SSSPQueryValues query) {
		final File outFolder = new File(outputDir);
		final File[] outFiles = outFolder.listFiles();
		final Map<Integer, Pair<Integer, Double>> vertices = new HashMap<>();

		// Get vertices
		for (final File f : outFiles) {
			try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String[] split0 = line.split("\t");
					String[] valueSplit = split0[1].split(":");
					vertices.put(Integer.parseInt(split0[0]),
							new Pair<>(Integer.parseInt(valueSplit[0]), Double.parseDouble(valueSplit[1])));
				}
			}
			catch (final Exception e) {
				logger.error("combine output failed", e);
				return;
			}
		}

		if (Configuration.getPropertyBoolDefault("KeepWorkerOutput", false)) {
			// Output combined, parse vertices
			try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "combined.txt"))) {
				for (final File f : outFiles) {
					try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
						String line;
						while ((line = reader.readLine()) != null) {
							String[] split0 = line.split("\t");
							String[] valueSplit = split0[1].split(":");
							vertices.put(Integer.parseInt(split0[0]),
									new Pair<>(Integer.parseInt(valueSplit[0]), Double.parseDouble(valueSplit[1])));
							writer.println(line);
						}
					}
				}
			}
			catch (final Exception e) {
				logger.error("combine output failed", e);
				return;
			}
		}
		else {
			for (final File f : outFiles) {
				f.delete();
			}
		}

		// Find target
		Entry<Integer, Pair<Integer, Double>> target = null;
		for (Entry<Integer, Pair<Integer, Double>> vertex : vertices.entrySet()) {
			if (vertex.getKey().equals(query.To)) {
				target = vertex;
			}
		}

		// Reconstruct path if target found
		if (target != null) {
			int nextVertex = target.getKey();
			Pair<Integer, Double> nextValue = target.getValue();
			try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + File.separator + "path.txt"))) {
				while (nextVertex != query.From) {
					writer.println(nextVertex + "\t" + nextValue.second);
					nextVertex = nextValue.first;
					nextValue = vertices.get(nextVertex);
				}
				writer.println(nextVertex);
			}
			catch (final Exception e) {
				logger.error("reconstruct path failed", e);
				return;
			}
		}
	}
}
