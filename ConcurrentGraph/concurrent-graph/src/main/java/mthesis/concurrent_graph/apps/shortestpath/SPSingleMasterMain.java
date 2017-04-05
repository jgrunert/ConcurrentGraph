package mthesis.concurrent_graph.apps.shortestpath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.apputils.MachineClusterConfiguration;
import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SPSingleMasterMain {

	private static final Logger logger = LoggerFactory.getLogger(SPSingleMasterMain.class);

	public static void main(String[] args) throws Exception {
		logger.info("Starting SSSP SingleMaster " + Configuration.VERSION);

		if (args.length < 4) {
			System.out.println("Usage: [configFile] [clusterConfigFile] [graphInputFile] [testSequence]");
			return;
		}

		Configuration.loadConfig(args[0]);
		final MachineClusterConfiguration config = new MachineClusterConfiguration(args[1]);
		final String inputFile = args[2];
		final String testSequenceFile = args[3];

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final SPConfiguration jobConfig = new SPConfiguration();
		final MasterInputPartitioner inputPartitioner = new RoadNetInputPartitioner(
				Configuration.getPropertyInt("PartitionsPerWorker"));
		final MasterOutputEvaluator<SPQuery> outputCombiner = new SPOutputEvaluator();

		// Start machines
		final RunUtils<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> testUtils = new RunUtils<>();
		MasterMachine<SPQuery> master = testUtils.startMaster(config.AllMachineConfigs, config.masterId,
				config.AllWorkerIds, inputFile,
				inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		// Run test sequence
		new SPTestSequenceRunner(master).runTestSequence(testSequenceFile);
		master.waitForAllQueriesFinish();
		master.stop();
	}
}
