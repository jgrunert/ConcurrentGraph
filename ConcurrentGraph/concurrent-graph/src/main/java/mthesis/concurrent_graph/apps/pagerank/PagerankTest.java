package mthesis.concurrent_graph.apps.pagerank;

import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.apputils.MachineClusterConfiguration;
import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.master.input.RoundRobinBlockInputPartitioner;
import mthesis.concurrent_graph.worker.VertexTextInputReader;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankTest {

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
		final PagerankJobConfiguration jobConfig = new PagerankJobConfiguration();
		// final MasterInputPartitioner inputPartitioner = new
		// ContinousBlockInputPartitioner(partitionSize);
		final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(partitionSize);
		final MasterOutputEvaluator<BaseQueryGlobalValues> outputCombiner = new PagerankOutputEvaluator();

		System.out.println("Starting machines");
		MasterMachine<BaseQueryGlobalValues> master = null;
		final RunUtils<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues> testUtils = new RunUtils<>();
		master = testUtils.startMaster(config.AllMachineConfigs, config.masterId,
				config.AllWorkerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		final List<WorkerMachine<DoubleWritable, NullWritable, DoubleWritable, BaseQueryGlobalValues>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			workers.add(testUtils.startWorker(config.AllMachineConfigs, i, config.masterId, config.AllWorkerIds,
					outputDir, jobConfig,
					new VertexTextInputReader<>()));
		}

		if (master != null) master.startQuery(new BaseQueryGlobalValues(0));
	}
}
