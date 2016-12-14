package mthesis.concurrent_graph.examples.cc;

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
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCTest {

	public static void main(String[] args) throws Exception {
		final int numWorkers = 4;
		final String host = "localhost";
		final int basePort = 23499;
		final String inputPartitionDir = "input";
		final String outputDir = "output";
		//final String inputFile = "../../Data_converted/cctest.txt";
		final String inputFile = "../../Data_converted/Wiki-Vote.txt";

		final CCDetectJobConfiguration jobConfig = new CCDetectJobConfiguration();
		final MasterInputPartitioner inputPartitioner = new ContinousBlockInputPartitioner(500);
		//final MasterInputPartitioner inputPartitioner = new RoundRobinBlockInputPartitioner(1);
		final MasterOutputEvaluator outputCombiner = new CCOutputWriter();

		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds= new ArrayList<>();
		allCfg.put(-1, new Pair<String, Integer>(host, basePort));
		for(int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new Pair<String, Integer>(host, basePort + 1 + i));
		}

		System.out.println("Starting");
		//final MasterNode master =
		startMaster(allCfg, -1, allWorkerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir);

		final List<WorkerMachine<IntWritable, NullWritable, IntWritable>> workers = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			workers.add(startWorker(allCfg, i, allWorkerIds, outputDir, jobConfig));
		}


		//		master.waitUntilStarted();
		//		worker0.waitUntilStarted();
		//		worker1.waitUntilStarted();
		//		System.out.println("All started");
		//		Thread.sleep(240000);

		//		System.out.println("Shutting down");
		//		master.stop();
		//		worker0.stop();
		//		worker1.stop();
		//		System.out.println("End");
	}

	private static WorkerMachine<IntWritable, NullWritable, IntWritable> startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String output,
			CCDetectJobConfiguration jobConfig) {
		final WorkerMachine<IntWritable, NullWritable, IntWritable> node = new WorkerMachine<IntWritable, NullWritable, IntWritable>(
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
