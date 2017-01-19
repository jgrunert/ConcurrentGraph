package mthesis.concurrent_graph.examples.sssp;

import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.examples.common.ExampleTestUtils;
import mthesis.concurrent_graph.examples.common.MachineClusterConfiguration;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPTest {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [inputFile] [partitionPerWorker]");
			return;
		}
		MachineClusterConfiguration config = new MachineClusterConfiguration(args[0]);
		final String inputFile = args[1];
		final int partitionPerWorker = Integer.parseInt(args[2]);

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();
		// final MasterInputPartitioner inputPartitioner = new
		// ContinousBlockInputPartitioner(partitionSize);
		final MasterInputPartitioner inputPartitioner = new RoadNetInputPartitioner(partitionPerWorker);
		final MasterOutputEvaluator<SSSPQueryValues> outputCombiner = new SSSPOutputEvaluator();

		// Start machines
		System.out.println("Starting machines");
		final ExampleTestUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> testUtils = new ExampleTestUtils<>();
		MasterMachine<SSSPQueryValues> master = null;
		if (config.StartOnThisMachine.get(config.masterId))
			master = testUtils.startMaster(config.AllMachineConfigs, config.masterId, config.AllWorkerIds, inputFile,
					inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		final List<WorkerMachine<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			if (config.StartOnThisMachine.get(config.AllWorkerIds.get(i))) workers
					.add(testUtils.startWorker(config.AllMachineConfigs, i, config.AllWorkerIds, outputDir, jobConfig,
							new RoadNetVertexInputReader()));
		}

		// Start query
		if (master != null) {
			// System.out.println("Starting query test");
			// Random rd = new Random(0);
			// for (int i = 0; i < 100; i++) {
			// int from = rd.nextInt(1090863);
			// int to = rd.nextInt(1090863);
			// master.startQuery(new SSSPQueryValues(i, from, to, 100));
			// }

			master.startQuery(new SSSPQueryValues(0, 2942985, 6663036));
			//			master.startQuery(new SSSPQueryValues(1, 0, 6310, 10));
			//			Thread.sleep(2000);
			//			master.startQuery(new SSSPQueryValues(2, 0, 6310, 10));
			master.waitForAllQueriesFinish();
			master.stop();
		}
	}
}
