package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.Configuration;

public class GreedyNewVertexMoveDecider<Q extends BaseQuery> extends AbstractVertexMoveDecider<Q> {

	private static final Logger logger = LoggerFactory.getLogger(GreedyNewVertexMoveDecider.class);

	// TODO Configuration
	private static final double WorkerImbalanceThreshold = 0.3;
	private static final long MinMoveWorkerVertices = 50;
	private static final long MinMoveTotalVertices = 500;

	private long vertexBarrierMoveLastTime = System.currentTimeMillis();


	@Override
	public VertexMoveDecision decide(Map<Integer, Map<Integer, Map<Integer, Integer>>> workerQueryIntersects) {

		if (!Configuration.VERTEX_BARRIER_MOVE_ENABLED
				|| (System.currentTimeMillis() - vertexBarrierMoveLastTime) < Configuration.VERTEX_BARRIER_MOVE_INTERVAL)
			return null;

		// Transform into queryMachines dataset
		Set<Integer> queryIds = new HashSet<>();
		List<Integer> workerIds = new ArrayList<>(workerQueryIntersects.keySet());
		Map<Integer, QueryWorkerMachine> queryMachines = new HashMap<>();
		for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> machine : workerQueryIntersects.entrySet()) {
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

		QueryDistribution originalDistribution = new QueryDistribution(queryIds, queryMachines);
		QueryDistribution bestDistribution = originalDistribution;
		System.out.println(bestDistribution.getCosts());

		System.out.println("/////////////////////////////////");
		//		bestDistribution.printMoveDistribution();

		//		QueryDistribution test = bestDistribution.clone();
		//		test.moveVertices(0, 1, 0);
		//		System.out.println(bestDistribution.getCosts());
		//		bestDistribution.printMoveDecissions();
		//		bestDistribution.printMoveDistribution();
		//
		//		test.moveVertices(0, 2, 0);
		//		System.out.println(bestDistribution.getCosts());
		//		bestDistribution.printMoveDecissions();
		//		bestDistribution.printMoveDistribution();
		//
		//		System.out.println("/////////////////////////////////");

		int totalVerticesMoved = 0;

		for (int i = 0; i < 100; i++) {
			QueryDistribution iterBestDistribution = bestDistribution.clone();
			boolean anyImproves = false;
			int verticesMovedIter = 0;

			//			System.out.println(i + " iteration");

			for (Integer queryId : queryIds) {
				for (Integer fromWorker : workerIds) {
					for (Integer toWorker : workerIds) {
						if (fromWorker == toWorker) continue;

						QueryDistribution newDistribution = bestDistribution.clone();
						int movedVerts = newDistribution.moveVertices(queryId, fromWorker, toWorker, true);

						double oldFmImbalance = bestDistribution.getWorkerImbalanceFactor(fromWorker);
						double oldToImbalance = bestDistribution.getWorkerImbalanceFactor(toWorker);
						double newFmImbalance = newDistribution.getWorkerImbalanceFactor(fromWorker);
						double newToImbalance = newDistribution.getWorkerImbalanceFactor(toWorker);
						boolean newFmBalanceBetter = newFmImbalance < oldFmImbalance;
						boolean newToBalanceBetter = newToImbalance < oldToImbalance;
						boolean newFmBalanceOk = newFmImbalance < WorkerImbalanceThreshold;
						boolean newToBalanceOk = newToImbalance < WorkerImbalanceThreshold;

						if (movedVerts > MinMoveWorkerVertices
								&& newDistribution.getCosts() < iterBestDistribution.getCosts()
								&& (newFmBalanceBetter || newFmBalanceOk) && (newToBalanceBetter || newToBalanceOk)) {
							iterBestDistribution = newDistribution;
							anyImproves = true;
							verticesMovedIter = movedVerts;
							//							System.out.println("## i " + i + ": " + newDistribution.getCosts());
							//							System.out
							//									.println("# " + newDistribution.getCosts() + " vs " + bestDistribution.getCosts());
						}
					}
				}
			}

			if (!anyImproves) {
				System.out.println("No more improves after " + i);
				break;
			}
			bestDistribution = iterBestDistribution;
			totalVerticesMoved += verticesMovedIter;
		}

		System.out.println("+++++++++++++");
		//		bestDistribution.printMoveDistribution();
		//		bestDistribution.printMoveDecissions();

		vertexBarrierMoveLastTime = System.currentTimeMillis();

		if (totalVerticesMoved < MinMoveTotalVertices) {
			logger.info("Decided not move, not enough vertices: " + totalVerticesMoved);
			return null;
		}
		logger.info("Decided move, vertices: " + totalVerticesMoved);
		return bestDistribution.toMoveDecision(workerIds);
	}
}
