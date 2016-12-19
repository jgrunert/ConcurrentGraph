package mthesis.concurrent_graph.examples.pagerank;

import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.examples.common.ExampleTestUtils;
import mthesis.concurrent_graph.examples.common.MachineClusterConfiguration;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.master.input.RoundRobinBlockInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankTest {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [inputFile] [partitionSize]");
		}
		MachineClusterConfiguration config = new MachineClusterConfiguration(args[0]);
		final String inputFile = args[1];
		final int partitionSize = Integer.parseInt(args[2]);

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final PagerankJobConfiguration jobConfig = new PagerankJobConfiguration();
		// final MasterInputPartitioner inputPartitioner = new
		// ContinousBlockInputPartitioner(partitionSize);
		final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(partitionSize);
		final MasterOutputEvaluator outputCombiner = new PagerankOutputEvaluator();

		System.out.println("Starting");
		final ExampleTestUtils<DoubleWritable, NullWritable, DoubleWritable> testUtils = new ExampleTestUtils<>();
		if (config.StartOnThisMachine.get(config.masterId))
			testUtils.startMaster(config.AllMachineConfigs, config.masterId, config.AllWorkerIds, inputFile,
					inputPartitionDir, inputPartitioner, outputCombiner, outputDir);

		final List<WorkerMachine<DoubleWritable, NullWritable, DoubleWritable>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			if (config.StartOnThisMachine.get(config.AllWorkerIds.get(i))) workers
					.add(testUtils.startWorker(config.AllMachineConfigs, i, config.AllWorkerIds, outputDir, jobConfig));
		}
	}
}
