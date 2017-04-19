package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
	private static final int MaxImproveIterations = 30;
	private static final long MaxImproveTime = Configuration.VERTEX_BARRIER_MOVE_INTERVAL / 2;



	@Override
	public VertexMoveDecision decide(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {

		List<Integer> workerIds = new ArrayList<>(queryMachines.keySet());

		QueryDistribution originalDistribution = new QueryDistribution(queryIds, queryMachines);
		QueryDistribution bestDistribution = originalDistribution;
		System.out.println(bestDistribution.getCosts());

		System.out.println("/////////////////////////////////");
		System.out.println(queryIds);
		System.out.println(queryMachines);
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

		int i = 0;
		long startTime = System.currentTimeMillis();
		for (; i < MaxImproveIterations && (System.currentTimeMillis() - startTime) < MaxImproveTime; i++) {
			QueryDistribution iterBestDistribution = bestDistribution.clone();
			boolean anyImproves = false;
			int verticesMovedIter = 0;

			//			System.out.println(i + " iteration");
			//			long startTime = System.currentTimeMillis();

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
				logger.info("No more improves after " + i);
				break;
			}
			bestDistribution = iterBestDistribution;
			totalVerticesMoved += verticesMovedIter;
		}
		logger.info("Stopped deciding after " + i + " iterations in " + (System.currentTimeMillis() - startTime) + "ms");

		System.out.println("+++++++++++++");
		//		bestDistribution.printMoveDistribution();
		//		bestDistribution.printMoveDecissions();

		if (totalVerticesMoved < MinMoveTotalVertices) {
			logger.info("Decided not move, not enough vertices: " + totalVerticesMoved);
			return null;
		}
		logger.info("Decided move, vertices: " + totalVerticesMoved);
		return bestDistribution.toMoveDecision(workerIds);
	}
}
