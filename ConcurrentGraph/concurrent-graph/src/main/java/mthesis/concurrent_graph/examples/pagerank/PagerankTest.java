package mthesis.concurrent_graph.examples.pagerank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.ContinousBlockInputPartitioner;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class PagerankTest {

	public static void main(String[] args) throws Exception {
		final int numWorkers = 4;
		final String host = "localhost";
		final int basePort = 23499;
		final String inputPartitionDir = "input";
		final String outputDir = "output";
		//final String inputFile = "../../Data_converted/cctest.txt";
		final String inputFile = "../../Data_converted/Wiki-Vote.txt";

		final PagerankJobConfiguration jobConfig = new PagerankJobConfiguration();
		final MasterInputPartitioner inputPartitioner = new ContinousBlockInputPartitioner(500);
		//final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(1);
		final MasterOutputEvaluator outputCombiner = new PagerankOutputEvaluator();

		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds= new ArrayList<>();
		allCfg.put(-1, new Pair<String, Integer>(host, basePort));
		for(int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new Pair<String, Integer>(host, basePort + 1 + i));
		}

		System.out.println("Starting");
		startMaster(allCfg, -1, allWorkerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir);

		final List<WorkerMachine<DoubleWritable, NullWritable, DoubleWritable>> workers = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			workers.add(startWorker(allCfg, i, allWorkerIds, outputDir, jobConfig));
		}
	}

	private static WorkerMachine<DoubleWritable, NullWritable, DoubleWritable> startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String output,
			PagerankJobConfiguration jobConfig) {
		final WorkerMachine<DoubleWritable, NullWritable, DoubleWritable> node = new WorkerMachine<DoubleWritable, NullWritable, DoubleWritable>(
				allCfg, id, allWorkers, -1, output, jobConfig);
		node.start();
		return node;
	}

	private static MasterMachine startMaster(Map<Integer, Pair<String, Integer>> machines, int ownId, List<Integer> workerIds,
			String inputFile, String inputPartitionDir, MasterInputPartitioner inputPartitioner,
			MasterOutputEvaluator outputCombiner, String outputDir) {
		final MasterMachine node = new MasterMachine(machines, ownId, workerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir);
		node.start();
		return node;
	}
}
