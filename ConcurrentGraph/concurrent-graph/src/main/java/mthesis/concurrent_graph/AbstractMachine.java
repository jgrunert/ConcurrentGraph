package mthesis.concurrent_graph;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.logging.ErrWarnCounter;
import mthesis.concurrent_graph.writable.BaseWritable;


// TODO Robustness, timeouts, superstep counters etc.
/**
 * Base class for nodes (master, worker). Handles messaging.
 * 
 * @author Jonas Grunert
 */
public abstract class AbstractMachine<M extends BaseWritable> {
	protected final Logger logger;
	//private final Map<Integer, Pair<String, Integer>> machines;
	protected final int ownId;

	protected final MessageSenderAndReceiver<M> messaging;
	protected final BlockingQueue<ControlMessage> inControlMessages = new LinkedBlockingQueue<>();

	private Thread runThread;


	protected AbstractMachine(Map<Integer, MachineConfig> machines, int ownId, BaseWritable.BaseWritableFactory<M> vertexMessageFactory) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		//this.machines = machines;
		this.ownId = ownId;
		this.messaging = new MessageSenderAndReceiver<>(machines, ownId, this, vertexMessageFactory);
	}

	public void start() {
		messaging.startServer();
		runThread = new Thread(new Runnable() {

			@Override
			public void run() {
				if(!messaging.startChannels() || !messaging.waitUntilConnected()) {
					logger.error("Connecting node failed");
					stop();
					return;
				}
				AbstractMachine.this.run();

				ErrWarnCounter.Enabled = false;
				if(ErrWarnCounter.Warnings > 0)
					logger.warn("Warnings: " + ErrWarnCounter.Warnings);
				if(ErrWarnCounter.Errors > 0)
					logger.warn("Errors: " + ErrWarnCounter.Errors);
				if(ErrWarnCounter.Warnings == 0 && ErrWarnCounter.Errors == 0)
					logger.info("No warnings or errors");
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


	public void onIncomingControlMessage(ControlMessage message) {
		inControlMessages.add(message);
	}

	public abstract void onIncomingVertexMessage(int msgSuperstepNo, int srcMachine, int srcVertex, int dstVertex, M messageContent);
}
