package mthesis.concurrent_graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.examples.CCDetectVertex;
import mthesis.concurrent_graph.examples.CCOutputWriter;
import mthesis.concurrent_graph.examples.EdgeListReader;
import mthesis.concurrent_graph.master.BaseMasterOutputCombiner;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.input.BaseInputPartitionDistributor;
import mthesis.concurrent_graph.master.input.BaseMasterInputReader;
import mthesis.concurrent_graph.master.input.ContinousInputPartitionDistributor;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;
import mthesis.concurrent_graph.writable.IntWritable;

public class QuickTest {

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
		final BaseMasterOutputCombiner outputCombiner = new CCOutputWriter();

		final Class<? extends AbstractVertex<IntWritable, IntWritable>> vertexClass = CCDetectVertex.class;



		//Thread.sleep(10000);


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

		final List<WorkerMachine<IntWritable, IntWritable>> workers = new ArrayList<>();
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

	private static WorkerMachine<IntWritable, IntWritable> startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String output,
			Class<? extends AbstractVertex<IntWritable, IntWritable>> vertexClass,
					BaseWritableFactory<IntWritable> vertexMessageFactory) {
		final WorkerMachine<IntWritable, IntWritable> node = new WorkerMachine<IntWritable, IntWritable>(
				allCfg, id, allWorkers, -1, output, vertexClass, vertexMessageFactory);
		node.start();
		return node;
	}

	private static MasterMachine startMaster(Map<Integer, Pair<String, Integer>> allMachines, int id, List<Integer> workerIds,
			BaseMasterInputReader inputReader, String inputFile, BaseInputPartitionDistributor inputDistributor,
			BaseMasterOutputCombiner outputCombiner, String outputDir) {
		final MasterMachine node = new MasterMachine(allMachines, id, workerIds, inputReader, inputFile, inputDistributor, outputCombiner, outputDir);
		node.start();
		return node;
	}
}
