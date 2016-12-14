package mthesis.concurrent_graph.examples.cc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.master.BaseMasterOutputEvaluator;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.input.BaseInputPartitionDistributor;
import mthesis.concurrent_graph.master.input.BaseMasterInputReader;
import mthesis.concurrent_graph.master.input.ContinousInputPartitionDistributor;
import mthesis.concurrent_graph.master.input.EdgeListReader;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class SCCTest {

	public static void main(String[] args) throws Exception {
		final int numWorkers = 4;
		final String host = "localhost";
		final int basePort = 23499;
		final String inputDir = "input";
		final String outputDir = "output";


		//		final String inputFile = "../../Data/cctest.txt";
		//		final BaseMasterInputReader inputReader = new VertexEdgesInputReader(1, inputDir);
		//		final BaseInputPartitionDistributor inputDistributor = new ContinousInputPartitionDistributor();
		//		final BaseMasterOutputCombiner outputCombiner = new CCOutputWriter();

		final String inputFile = "../../Data/Wiki-Vote.txt";
		final BaseMasterInputReader inputReader = new EdgeListReader(4000, inputDir);
		final BaseInputPartitionDistributor inputDistributor = new ContinousInputPartitionDistributor();
		final BaseMasterOutputEvaluator outputCombiner = new CCOutputWriter();

		final Class<? extends AbstractVertex<IntWritable, NullWritable, IntWritable>> vertexClass = CCDetectVertex.class;


		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds= new ArrayList<>();
		allCfg.put(-1, new Pair<String, Integer>(host, basePort));
		for(int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new Pair<String, Integer>(host, basePort + 1 + i));
		}

		System.out.println("Starting");
		//final MasterNode master =
		startMaster(allCfg, -1, allWorkerIds, inputReader, inputFile, inputDistributor, outputCombiner, outputDir);

		final List<WorkerMachine<IntWritable, NullWritable, IntWritable>> workers = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			workers.add(startWorker(allCfg, i, allWorkerIds, outputDir, vertexClass, new IntWritable.Factory()));
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
			Class<? extends AbstractVertex<IntWritable, NullWritable, IntWritable>> vertexClass,
					BaseWritableFactory<IntWritable> vertexMessageFactory) {
		final WorkerMachine<IntWritable, NullWritable, IntWritable> node = new WorkerMachine<IntWritable, NullWritable, IntWritable>(
				allCfg, id, allWorkers, -1, output, vertexClass, vertexMessageFactory);
		node.start();
		return node;
	}

	private static MasterMachine startMaster(Map<Integer, Pair<String, Integer>> allMachines, int id, List<Integer> workerIds,
			BaseMasterInputReader inputReader, String inputFile, BaseInputPartitionDistributor inputDistributor,
			BaseMasterOutputEvaluator outputCombiner, String outputDir) {
		final MasterMachine node = new MasterMachine(allMachines, id, workerIds, inputReader, inputFile, inputDistributor, outputCombiner, outputDir);
		node.start();
		return node;
	}
}
