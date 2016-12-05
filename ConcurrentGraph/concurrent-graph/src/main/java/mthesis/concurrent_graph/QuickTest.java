package mthesis.concurrent_graph;

import java.util.HashMap;
import java.util.Map;

import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		Pair<String, Integer> masterCfg = new Pair<String, Integer>("localhost", 23499);
		Pair<String, Integer> worker0Cfg = new Pair<String, Integer>("localhost", 23500);
		Pair<String, Integer> worker1Cfg = new Pair<String, Integer>("localhost", 23501);
		
		Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		allCfg.put(-1, masterCfg);
		allCfg.put(0, worker0Cfg);
		allCfg.put(1, worker1Cfg);
		
		MessageSenderAndReceiver masterMsg = new MessageSenderAndReceiver(allCfg, -1);
		MessageSenderAndReceiver worker0Msg = new MessageSenderAndReceiver(allCfg, 0);
		MessageSenderAndReceiver worker1Msg = new MessageSenderAndReceiver(allCfg, 1);
		
		masterMsg.start();
		worker0Msg.start();
		worker1Msg.start();
		
		Thread.sleep(5000);

		System.out.println("Shutting down");
		masterMsg.stop();
		worker0Msg.stop();
		worker1Msg.stop();
		System.out.println("End");
	}
}
