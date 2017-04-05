package mthesis.concurrent_graph;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.ChannelMessage;
import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable;


// TODO Robustness, timeouts, superstep counters etc.
/**
 * Base class for nodes (master, worker). Handles messaging.
 *
 * @author Jonas Grunert
 */
public abstract class AbstractMachine<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	protected final Logger logger;
	//private final Map<Integer, Pair<String, Integer>> machines;
	protected final int ownId;

	protected final MessageSenderAndReceiver<V, E, M, Q> messaging;
	private Thread runThread;



	protected AbstractMachine(Map<Integer, MachineConfig> machines, int ownId,
			JobConfiguration<V, E, M, Q> jobConfig) {
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");

		//this.machines = machines;
		this.ownId = ownId;
		// Hack to get worker interface
		VertexWorkerInterface<V, E, M, Q> worker = (this instanceof WorkerMachine<?, ?, ?, ?>) ? (WorkerMachine<V, E, M, Q>) this : null;
		this.messaging = new MessageSenderAndReceiver<V, E, M, Q>(machines, ownId, worker, this, jobConfig);
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
				try {
					AbstractMachine.this.run();
				}
				catch (Throwable e) {
					logger.error("Exception at run", e);
				}
				finally {
					logger.debug("Finished run");
				}
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


	public abstract void onIncomingMessage(ChannelMessage message);
}
