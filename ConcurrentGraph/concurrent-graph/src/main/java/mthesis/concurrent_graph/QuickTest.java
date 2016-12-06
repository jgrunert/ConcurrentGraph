package mthesis.concurrent_graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.worker.WorkerNode;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		Pair<String, Integer> masterCfg = new Pair<String, Integer>("localhost", 23499);
		Pair<String, Integer> worker0Cfg = new Pair<String, Integer>("localhost", 23500);
		Pair<String, Integer> worker1Cfg = new Pair<String, Integer>("localhost", 23501);
		
		Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		//allCfg.put(-1, masterCfg);
		allCfg.put(0, worker0Cfg);
		allCfg.put(1, worker1Cfg);
		List<Integer> allWorkers = Arrays.asList(0, 1);
		
		//MessageSenderAndReceiver master = startWorker(allCfg, -1);
		WorkerNode worker0 = startWorker(allCfg, 0, allWorkers, new HashSet<Integer>(Arrays.asList(1, 2, 6, 7)), "../../Data/cctest.txt");
		WorkerNode worker1 = startWorker(allCfg, 1, allWorkers, new HashSet<Integer>(Arrays.asList(3, 4, 5)), "../../Data/cctest.txt");

		worker0.waitStarted();
		worker1.waitStarted();
		
		Thread.sleep(5000);

		System.out.println("Shutting down");
		//master.stop();
		worker0.stop();
		worker1.stop();
		System.out.println("End");
	}
	
	private static WorkerNode startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, Set<Integer> vertexIds, String dataDir) {
		WorkerNode workerNode = new WorkerNode(allCfg, id, allWorkers, vertexIds, dataDir);
		workerNode.start();
		return workerNode;
	}
}
