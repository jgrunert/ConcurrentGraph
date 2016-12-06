package mthesis.concurrent_graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.worker.WorkerNode;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		final Pair<String, Integer> masterCfg = new Pair<String, Integer>("localhost", 23499);
		final Pair<String, Integer> worker0Cfg = new Pair<String, Integer>("localhost", 23500);
		final Pair<String, Integer> worker1Cfg = new Pair<String, Integer>("localhost", 23501);

		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		//allCfg.put(-1, masterCfg);
		allCfg.put(0, worker0Cfg);
		allCfg.put(1, worker1Cfg);
		final List<Integer> allWorkers = Arrays.asList(0, 1);

		//MessageSenderAndReceiver master = startWorker(allCfg, -1);
		final WorkerNode worker0 = startWorker(allCfg, 0, allWorkers, new HashSet<Integer>(Arrays.asList(1, 2, 6, 7)), "../../Data/cctest.txt");
		final WorkerNode worker1 = startWorker(allCfg, 1, allWorkers, new HashSet<Integer>(Arrays.asList(3, 4, 5)), "../../Data/cctest.txt");

		worker0.waitUntilStarted();
		worker1.waitUntilStarted();

		Thread.sleep(5000);

		System.out.println("Shutting down");
		//master.stop();
		worker0.stop();
		worker1.stop();
		System.out.println("End");
	}

	private static WorkerNode startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, Set<Integer> vertexIds, String dataDir) {
		final WorkerNode workerNode = new WorkerNode(allCfg, id, allWorkers, vertexIds, dataDir);
		workerNode.start();
		return workerNode;
	}
}
