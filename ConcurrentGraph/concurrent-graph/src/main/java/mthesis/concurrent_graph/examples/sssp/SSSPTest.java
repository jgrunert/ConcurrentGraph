package mthesis.concurrent_graph.examples.sssp;

import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.QueryGlobalValues;
import mthesis.concurrent_graph.examples.common.ExampleTestUtils;
import mthesis.concurrent_graph.examples.common.MachineClusterConfiguration;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.master.input.RoundRobinBlockInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPTest {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [inputFile] [partitionSize]");
			return;
		}
		MachineClusterConfiguration config = new MachineClusterConfiguration(args[0]);
		final String inputFile = args[1];
		final int partitionSize = Integer.parseInt(args[2]);

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();
		// final MasterInputPartitioner inputPartitioner = new
		// ContinousBlockInputPartitioner(partitionSize);
		final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(partitionSize);
		final MasterOutputEvaluator outputCombiner = new SSSPOutputEvaluator();

		System.out.println("Starting");
		final ExampleTestUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, QueryGlobalValues> testUtils = new ExampleTestUtils<>();
		if (config.StartOnThisMachine.get(config.masterId)) testUtils.startMaster(config.AllMachineConfigs, config.masterId,
				config.AllWorkerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		final List<WorkerMachine<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, QueryGlobalValues>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			if (config.StartOnThisMachine.get(config.AllWorkerIds.get(i)))
				workers.add(testUtils.startWorker(config.AllMachineConfigs, i, config.AllWorkerIds, outputDir, jobConfig));
		}
	}
}
