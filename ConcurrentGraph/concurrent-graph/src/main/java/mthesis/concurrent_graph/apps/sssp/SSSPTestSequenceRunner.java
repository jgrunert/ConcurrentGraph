package mthesis.concurrent_graph.apps.sssp;

import java.io.BufferedReader;
import java.io.FileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.master.MasterMachine;

public class SSSPTestSequenceRunner {

	private static final Logger logger = LoggerFactory.getLogger(SSSPTestSequenceRunner.class);
	private final MasterMachine<SSSPQueryValues> master;


	public SSSPTestSequenceRunner(MasterMachine<SSSPQueryValues> master) {
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
						master.startQuery(new SSSPQueryValues(queryIndex++, Integer.parseInt(lineSplit[1]),
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
