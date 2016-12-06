package mthesis.concurrent_graph;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import mthesis.concurrent_graph.master.MasterNode;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.worker.WorkerNode;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		final String input = "../../Data/cctest.txt";
		final String output = "output";
		final int numWorkers = 3;
		final String host = "localhost";
		final int basePort = 23499;

		final File outFile = new File(output);
		if(outFile.exists())
			for(final File f : outFile.listFiles())
				f.delete();
		else
			outFile.mkdirs();

		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds= new ArrayList<>();
		allCfg.put(-1, new Pair<String, Integer>(host, basePort));
		for(int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new Pair<String, Integer>(host, basePort + 1 + i));
		}

		final List<Integer> graphNodes = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
		final int totalNodes = graphNodes.size();
		final int nodesPerWorker = (totalNodes + numWorkers - 1) / numWorkers;
		final Random random = new Random(0);

		System.out.println("Starting");
		//final MasterNode master =
		startMaster(allCfg, -1, allWorkerIds, output);

		final List<WorkerNode> workers = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			final Set<Integer> workerNodes = new HashSet<>();
			for(int j = 0; j < nodesPerWorker && !graphNodes.isEmpty(); j++) {
				workerNodes.add(graphNodes.remove(random.nextInt(graphNodes.size())));
			}
			workers.add(startWorker(allCfg, i, allWorkerIds, workerNodes, input, output));
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
			int id, List<Integer> allWorkers, Set<Integer> vertexIds, String input, String output) {
		final WorkerNode node = new WorkerNode(allCfg, id, allWorkers, -1, vertexIds, input, output);
		node.start();
		return node;
	}

	private static MasterNode startMaster(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String output) {
		final MasterNode node = new MasterNode(allCfg, id, allWorkers, output);
		node.start();
		return node;
	}
}
