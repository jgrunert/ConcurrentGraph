package mthesis.concurrent_graph.master.vertexmove;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.Configuration;

public class VertexMoveDeciderService<Q extends BaseQuery> {

	private static final Logger logger = LoggerFactory.getLogger(VertexMoveDeciderService.class);

	private final AbstractVertexMoveDecider<Q> moveDecider;
	private final Thread deciderThread;
	private boolean stopRequested;

	private Object workerQueryIntersectsLock = new Object();
	private volatile boolean newQueryIntersects;
	/** Map<Machine, Map<QueryId, Map<IntersectQueryId, IntersectingsCount>>> */
	private Map<Integer, Map<Integer, Map<Integer, Integer>>> latestWorkerQueryIntersects;

	private long vertexBarrierMoveLastTime = System.currentTimeMillis();
	private Object latestDecissionLock = new Object();
	private volatile boolean newDecissionFinished;
	private VertexMoveDecision latestDecission;

	public VertexMoveDeciderService(AbstractVertexMoveDecider<Q> moveDecider) {
		this.moveDecider = moveDecider;
		stopRequested = false;
		deciderThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					deciderLoop();
				}
				catch (Exception e) {
					if (!stopRequested) logger.error("VertexMoveDeciderThread crash", e);
				}
			}
		});
		deciderThread.setName("VertexMoveDeciderThread");
		deciderThread.setDaemon(true);
	}

	public void start() {
		deciderThread.start();
	}

	public void stop() {
		stopRequested = true;
		deciderThread.interrupt();
	}


	public void updateQueryIntersects(Map<Integer, Map<Integer, Map<Integer, Integer>>> workerQueryIntersects) {
		synchronized (workerQueryIntersectsLock) {
			latestWorkerQueryIntersects = workerQueryIntersects;
			newQueryIntersects = true;
		}
	}

	public boolean hasNewDecission() {
		return newDecissionFinished;
	}

	public VertexMoveDecision getNewDecission() {
		synchronized (latestDecissionLock) {
			newDecissionFinished = false;
			return latestDecission;
		}
	}

	private void deciderLoop() throws Exception {
		while (!Thread.interrupted() && !stopRequested) {
			long elapsed = System.currentTimeMillis() - vertexBarrierMoveLastTime;
			if (elapsed >= Configuration.VERTEX_BARRIER_MOVE_INTERVAL && newQueryIntersects) {
				newQueryIntersects = false;
				long decideStartTime = System.currentTimeMillis();
				newMoveDecission();
				logger.info("Decided to move in " + (System.currentTimeMillis() - decideStartTime));
				// TODO Master stats decide time
				vertexBarrierMoveLastTime = System.currentTimeMillis();
			}
			else {
				Thread.sleep(Math.max(1, Configuration.VERTEX_BARRIER_MOVE_INTERVAL - elapsed));
			}
		}
	}

	private void newMoveDecission() {
		// Transform into queryMachines dataset
		Set<Integer> queryIds = new HashSet<>();
		Map<Integer, QueryWorkerMachine> queryMachines = new HashMap<>();
		synchronized (workerQueryIntersectsLock) {
			// Transform into queryMachines dataset
			for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> machine : latestWorkerQueryIntersects.entrySet()) {
				Map<Integer, QueryVerticesOnMachine> machineQueries = new HashMap<>();
				for (Entry<Integer, Map<Integer, Integer>> machineQuery : machine.getValue().entrySet()) {
					int queryId = machineQuery.getKey();
					queryIds.add(queryId);
					int totalVertices = machineQuery.getValue().get(queryId);
					Map<Integer, Integer> intersects = new HashMap<>(machineQuery.getValue());
					intersects.remove(queryId);
					machineQueries.put(queryId, new QueryVerticesOnMachine(totalVertices, intersects));
				}
				queryMachines.put(machine.getKey(), new QueryWorkerMachine(machineQueries));
			}
		}

		VertexMoveDecision newDecission = moveDecider.decide(queryIds, queryMachines);
		synchronized (latestDecissionLock) {
			latestDecission = newDecission;
			newDecissionFinished = true;
		}
	}
}
