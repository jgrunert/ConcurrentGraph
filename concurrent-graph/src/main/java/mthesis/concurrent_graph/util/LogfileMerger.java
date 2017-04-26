package mthesis.concurrent_graph.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LogfileMerger {

	public static void main(String[] args) throws Exception {
		String logfileDir = args[0];
		System.out.println(logfileDir);

		List<String> logLines = new ArrayList<>();
		for (File logfile : new File(logfileDir).listFiles()) {
			try (BufferedReader reader = new BufferedReader(new FileReader(logfile))) {
				String line;
				while ((line = reader.readLine()) != null) {
					logLines.add(line);
				}
			}
		}

		Collections.sort(logLines);
		String logMergedFile = logfileDir + File.separator + "log_merged.txt";
		try (PrintWriter writer = new PrintWriter(new FileWriter(logMergedFile))) {
			for (String line : logLines) {
				writer.println(line);
			}
		}
		System.out.println("Logs merged to " + logMergedFile);
	}
}
