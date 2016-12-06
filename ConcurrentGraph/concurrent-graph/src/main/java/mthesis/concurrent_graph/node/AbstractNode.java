package mthesis.concurrent_graph.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.ControlMessage;
import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.communication.MessageType;
import mthesis.concurrent_graph.communication.VertexMessage;
import mthesis.concurrent_graph.util.Pair;


// TODO Robustness, timeouts, superstep counters etc.
/**
 * Base class for nodes (master, worker). Handles messaging.
 * 
 * @author Jonas Grunert
 */
public abstract class AbstractNode {
	protected final Logger logger;
	//private final Map<Integer, Pair<String, Integer>> machines;
	protected final int ownId;
	
	private final MessageSenderAndReceiver messaging;
	protected final BlockingQueue<VertexMessage> inWorkerMessages = new LinkedBlockingQueue<>();
	protected final Map<MessageType, List<ControlMessage>> inControlMessages = new HashMap<>();
	
	private Thread runThread;

	
	protected AbstractNode(Map<Integer, Pair<String, Integer>> machines, int ownId) {
		this.logger = LoggerFactory.getLogger(this.getClass() + "[" + ownId + "]");
		//this.machines = machines;
		this.ownId = ownId;
		this.messaging = new MessageSenderAndReceiver(machines, ownId, this);
	}
	
	public void start() {
		messaging.start();
		runThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				AbstractNode.this.run();
			}
		});
		runThread.setName("NodeThread[" + ownId + "]");
		runThread.start();
	}
	
	public void waitUntilStarted() {		
		messaging.waitUntilStarted();	
	}
	
	public void stop() {
		messaging.stop();
		if(runThread != null)
			runThread.interrupt();
	}
	
	public abstract void run();

	
	public void onIncomingMessage(String message) {
		logger.trace(message);
		
		String[] msgSplit = message.split(";");
		MessageType type = MessageType.valueOf(msgSplit[0]);
		int superstepNo = Integer.parseInt(msgSplit[1]);

		if (type == MessageType.Worker) {
			int fromVertex = Integer.parseInt(msgSplit[2]);
			int toVertex = Integer.parseInt(msgSplit[3]);
			inWorkerMessages.add(new VertexMessage(fromVertex, toVertex, superstepNo, msgSplit[3]));
		} else {
			synchronized (inControlMessages) {
				List<ControlMessage> messages = inControlMessages.get(type);
				if (messages == null) {
					messages = new ArrayList<>();
					inControlMessages.put(type, messages);
				}
				messages.add(new ControlMessage(superstepNo, msgSplit[2]));
			}
		}
	}

	
	public void broadcastControlMessage(MessageType type, int superstepNo, String content) {
		messaging.sendMessageToAll(type + ";" + superstepNo + ";" + content);		
	}
		
	public void sendVertexMessage(int fromVertex, int toVertex, int superstepNo, String content) {
		messaging.sendMessageToAll(MessageType.Worker + ";" + superstepNo + ";" + fromVertex + ";" + toVertex + ";" + content);			
		inWorkerMessages.add(new VertexMessage(fromVertex, toVertex, superstepNo, content));
	}
}
