package mthesis.concurrent_graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.examples.CCDetectVertex;
import mthesis.concurrent_graph.examples.EdgeListReader;
import mthesis.concurrent_graph.master.AbstractMasterInputReader;
import mthesis.concurrent_graph.master.MasterNode;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.WorkerNode;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		final String inputData = "../../Data/cctest.txt";
		final String inputDir = "input";
		final String outputDir = "output";
		final int numWorkers = 3;
		final String host = "localhost";
		final int basePort = 23499;
		final Class<? extends AbstractMasterInputReader> inputReader = EdgeListReader.class;
		//final Class<? extends AbstractMasterOutputWriter> outputWriter = EdgeListReader.class;
		final Class<? extends AbstractVertex> vertexClass = CCDetectVertex.class;

		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds= new ArrayList<>();
		allCfg.put(-1, new Pair<String, Integer>(host, basePort));
		for(int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new Pair<String, Integer>(host, basePort + 1 + i));
		}

		System.out.println("Starting");
		//final MasterNode master =
		startMaster(allCfg, -1, allWorkerIds, inputData, inputDir, outputDir, inputReader);

		final List<WorkerNode> workers = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			workers.add(startWorker(allCfg, i, allWorkerIds, inputDir + File.separator + i + ".txt", outputDir, vertexClass));
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

	private static WorkerNode startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String input, String output,
			Class<? extends AbstractVertex> vertexClass) {
		final WorkerNode node = new WorkerNode(allCfg, id, allWorkers, -1, input, output, vertexClass);
		node.start();
		return node;
	}

	private static MasterNode startMaster(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String inputData, String inputDir, String outputDir,
			Class<? extends AbstractMasterInputReader> inputReader) {
		final MasterNode node = new MasterNode(allCfg, id, allWorkers, inputData, inputDir, outputDir, inputReader);
		node.start();
		return node;
	}
}
