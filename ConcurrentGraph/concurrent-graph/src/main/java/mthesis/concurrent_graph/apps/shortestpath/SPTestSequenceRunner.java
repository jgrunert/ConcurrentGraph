package mthesis.concurrent_graph.apps.shortestpath;

import java.io.BufferedReader;
import java.io.FileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.master.MasterMachine;

public class SPTestSequenceRunner {

	private static final Logger logger = LoggerFactory.getLogger(SPTestSequenceRunner.class);
	private final MasterMachine<SPQuery> master;


	public SPTestSequenceRunner(MasterMachine<SPQuery> master) {
		this.master = master;
	}


	public void runTestSequence(String sequenceFile) {
		int queryIndex = 0;

		try (BufferedReader sequenceReader = new BufferedReader(new FileReader(sequenceFile))) {
			String line;
			while ((line = sequenceReader.readLine()) != null) {
				String[] lineSplit = line.split("\t");
				logger.info("Execute testsequence command " + line);
				switch (lineSplit[0]) {
					case "wait":
						Thread.sleep(Integer.parseInt(lineSplit[1]));
						break;
					case "waitall":
						master.waitForAllQueriesFinish();
						break;
					case "start":
						master.startQuery(new SPQuery(queryIndex++, Integer.parseInt(lineSplit[1]),
								Integer.parseInt(lineSplit[2])));
						break;
					default:
						logger.warn("Invalid line at run sequence: " + line);
						break;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
