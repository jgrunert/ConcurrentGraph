package mthesis.concurrent_graph.node;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	protected final MessageSenderAndReceiver messaging;
	protected final BlockingQueue<VertexMessage> inVertexMessages = new LinkedBlockingQueue<>();
	protected final BlockingQueue<ControlMessage> inControlMessages = new LinkedBlockingQueue<>();

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
				if(!messaging.waitUntilConnected()) {
					logger.error("Connecting node failed");
					return;
				}
				AbstractNode.this.run();
			}
		});
		runThread.setName("NodeThread[" + ownId + "]");
		runThread.start();
	}


	public void stop() {
		messaging.stop();
		if(runThread != null)
			runThread.interrupt();
	}

	public abstract void run();


	public void onIncomingMessage(String message) {
		//logger.trace(message);

		final String[] msgSplit = message.split(";");
		final MessageType type = MessageType.valueOf(msgSplit[0]);
		final int fromNode = Integer.parseInt(msgSplit[1]);
		final int superstepNo = Integer.parseInt(msgSplit[2]);

		if (type == MessageType.Vertex) {
			logger.trace("Vertex message: " + message);
			final int fromVertex = Integer.parseInt(msgSplit[3]);
			final int toVertex = Integer.parseInt(msgSplit[4]);
			inVertexMessages.add(new VertexMessage(fromNode, fromVertex, toVertex, superstepNo, msgSplit[5]));
		} else {
			logger.trace("Control message: " + message);
			inControlMessages.add(new ControlMessage(type, fromNode, superstepNo, msgSplit[3]));
		}
	}
}
