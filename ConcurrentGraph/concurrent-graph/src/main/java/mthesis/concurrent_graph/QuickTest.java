package mthesis.concurrent_graph;

import java.util.HashMap;
import java.util.Map;

import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.util.Pair;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		Pair<String, Integer> masterCfg = new Pair<String, Integer>("localhost", 23499);
		Pair<String, Integer> worker0Cfg = new Pair<String, Integer>("localhost", 23500);
		Pair<String, Integer> worker1Cfg = new Pair<String, Integer>("localhost", 23501);
		
		Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		allCfg.put(-1, masterCfg);
		allCfg.put(0, worker0Cfg);
		allCfg.put(1, worker1Cfg);
		
		MessageSenderAndReceiver master = startWorker(allCfg, -1);
		MessageSenderAndReceiver worker0 = startWorker(allCfg, 0);
		MessageSenderAndReceiver worker1 = startWorker(allCfg, 1);
		
		Thread.sleep(5000);

		System.out.println("Shutting down");
		master.stop();
		worker0.stop();
		worker1.stop();
		System.out.println("End");
	}
	
	private static MessageSenderAndReceiver startWorker(Map<Integer, Pair<String, Integer>> allCfg, int id) {
		final MessageSenderAndReceiver messaging = new MessageSenderAndReceiver(allCfg, id);
		Thread workerThread = new Thread(new Runnable() {			
			@Override
			public void run() {
				messaging.start();
				messaging.sendMessageToAll(id + " to all");
				messaging.sendMessageToAll(id + " to all 2");
			}
		});
		workerThread.setName("WorkerThread_[" + id + "]");
		workerThread.start();
		
		return messaging;
	}
}
