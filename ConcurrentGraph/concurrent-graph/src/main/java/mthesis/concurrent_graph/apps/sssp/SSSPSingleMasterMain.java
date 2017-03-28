package mthesis.concurrent_graph.apps.sssp;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.apputils.MachineClusterConfiguration;
import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPSingleMasterMain {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: [clusterConfigFile] [inputFile]");
			return;
		}
		final MachineClusterConfiguration config = new MachineClusterConfiguration(args[0]);
		final String inputFile = args[1];

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();
		final MasterInputPartitioner inputPartitioner = new RoadNetInputPartitioner(
				Configuration.getPropertyInt("PartitionsPerWorker"));
		final MasterOutputEvaluator<SSSPQueryValues> outputCombiner = new SSSPOutputEvaluator();

		// Start machines
		System.out.println("Starting machines");
		final RunUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> testUtils = new RunUtils<>();
		MasterMachine<SSSPQueryValues> master = testUtils.startMaster(config.AllMachineConfigs, config.masterId,
				config.AllWorkerIds, inputFile,
				inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		// TODO Better test or external tests
		int queryIndex = 0;
		// Test sequence
		//		master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821)); // Medium PF->HB
		//		master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011)); // Short Heidelberg->Heilbronn
		//		master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832)); // Short RT->ST
		master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 4982624)); // Short ST-HBF->TU
		master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337)); // Very short Meersburg->Pfullendorf
	}
}
