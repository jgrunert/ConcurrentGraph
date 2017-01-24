package mthesis.concurrent_graph;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.util.Pair;
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

	private Thread runThread;


	protected AbstractMachine(Map<Integer, MachineConfig> machines, int ownId,
			BaseWritable.BaseWritableFactory<M> vertexMessageFactory) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
		//this.machines = machines;
		this.ownId = ownId;
		this.messaging = new MessageSenderAndReceiver<>(machines, ownId, this, vertexMessageFactory);
	}

	public void start() {
		messaging.startServer();
		if (!messaging.startChannels() || !messaging.waitUntilConnected()) {
			logger.error("Connecting node failed");
			stop();
			return;
		}

		runThread = new Thread(new Runnable() {

			@Override
			public void run() {
				AbstractMachine.this.run();
			}
		});
		runThread.setName("NodeThread[" + ownId + "]");
		runThread.start();
	}



	public void stop() {
		logger.info("Stopping machine");
		messaging.stop();
		if (runThread != null)
			runThread.interrupt();
		logger.info("Machine stopped");
	}

	public abstract void run();


	public abstract void onIncomingControlMessage(ControlMessage message);

	public abstract void onIncomingVertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages);

	public abstract void onIncomingGetToKnowMessage(int srcMachine, Collection<Integer> srcVertices, int queryId);
}
