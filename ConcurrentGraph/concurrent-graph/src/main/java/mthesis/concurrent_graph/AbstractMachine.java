package mthesis.concurrent_graph;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.communication.MessageSenderAndReceiver;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable;


// TODO Robustness, timeouts, superstep counters etc.
/**
 * Base class for nodes (master, worker). Handles messaging.
 *
 * @author Jonas Grunert
 */
public abstract class AbstractMachine<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

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

	public abstract void onIncomingMoveVerticesMessage(int srcMachine, Collection<AbstractVertex<V, E, M, Q>> srcVertices, int queryId,
			boolean lastSegment);

	public abstract void onIncomingInvalidateRegisteredVerticesMessage(int srcMachine, Collection<Integer> srcVertices, int queryId);
}
