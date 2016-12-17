package mthesis.concurrent_graph.examples.pagerank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.examples.common.ExampleTestUtils;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.master.input.RoundRobinBlockInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankTest {

	public static void main(String[] args) throws Exception {
		final int numWorkers = 4;
		final String host = "localhost";
		final int baseControlMsgPort = 23499;
		final String inputPartitionDir = "input";
		final String outputDir = "output";
		//final String inputFile = "../../Data_converted/cctest.txt";
		final String inputFile = "../../../ConcurrentGraph_Data/converted/web-Stanford.txt";

		final PagerankJobConfiguration jobConfig = new PagerankJobConfiguration();
		//final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(1);
		final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(10000);
		final MasterOutputEvaluator outputCombiner = new PagerankOutputEvaluator();

		final Map<Integer, MachineConfig> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds= new ArrayList<>();
		allCfg.put(-1, new MachineConfig(host, baseControlMsgPort));
		for(int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new MachineConfig(host, baseControlMsgPort + 1 + i));
		}

		System.out.println("Starting");
		final ExampleTestUtils<DoubleWritable, NullWritable, DoubleWritable> testUtils = new ExampleTestUtils<>();
		testUtils.startMaster(allCfg, -1, allWorkerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir);

		final List<WorkerMachine<DoubleWritable, NullWritable, DoubleWritable>> workers = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			workers.add(testUtils.startWorker(allCfg, i, allWorkerIds, outputDir, jobConfig));
		}
	}
}
