package mthesis.concurrent_graph.master.vertexmove;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.BaseQuery;

public class VertexMoveDeciderService<Q extends BaseQuery> {

	private static final Logger logger = LoggerFactory.getLogger(VertexMoveDeciderService.class);

	private final AbstractVertexMoveDecider<Q> moveDecider;
	private final Thread deciderThread;
	private boolean stopRequested;

	private Object workerQueryIntersectsLock = new Object();
	private volatile boolean newQueryIntersectsReady;
	private final IntSet workerIds;
	/** Map<Machine, Map<QueryId, Map<IntersectQueryId, IntersectingsCount>>> */
	private Map<Integer, Map<IntSet, Integer>> latestWorkerQueryChunks = new HashMap<>();

	//private long vertexBarrierMoveLastTime = System.currentTimeMillis();
	private Object latestDecissionLock = new Object();
	private volatile boolean newDecissionFinished;
	private VertexMoveDecision latestDecission;

	public VertexMoveDeciderService(AbstractVertexMoveDecider<Q> moveDecider, List<Integer> workerIds) {
		this.moveDecider = moveDecider;
		this.workerIds = new IntOpenHashSet(workerIds);
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


	public void updateQueryIntersects(int workerId, Map<IntSet, Integer> workerQueryIntersects) {
		synchronized (workerQueryIntersectsLock) {
			latestWorkerQueryChunks.put(workerId, workerQueryIntersects);
			if (latestWorkerQueryChunks.size() == workerIds.size()) {
				newQueryIntersectsReady = true;
			}
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
			//long elapsed = System.currentTimeMillis() - vertexBarrierMoveLastTime;
			if (newQueryIntersectsReady) {
				long decideStartTime = System.currentTimeMillis();
				calcNewMoveDecission();
				logger.info("Decided to move in " + (System.currentTimeMillis() - decideStartTime));
				// TODO Master stats decide time
				//vertexBarrierMoveLastTime = System.currentTimeMillis();
			}
			else {
				Thread.sleep(1);
			}
		}
	}

	private void calcNewMoveDecission() {
		// Transform into queryMachines dataset
		Set<Integer> queryIds = new HashSet<>();
		Map<Integer, Map<IntSet, Integer>> queryChunks;
		synchronized (workerQueryIntersectsLock) {
			queryChunks = new HashMap<>(latestWorkerQueryChunks);
			// Transform into queryMachines dataset
			for (Map<IntSet, Integer> workerChunks : latestWorkerQueryChunks.values()) {
				for (IntSet chunkQueries : workerChunks.keySet()) {
					queryIds.addAll(chunkQueries);
				}
			}
			latestWorkerQueryChunks.clear();
			newQueryIntersectsReady = false;
		}

		VertexMoveDecision newDecission = moveDecider.decide(queryIds, queryChunks);
		synchronized (latestDecissionLock) {
			latestDecission = newDecission;
			newDecissionFinished = true;
		}
	}
}
