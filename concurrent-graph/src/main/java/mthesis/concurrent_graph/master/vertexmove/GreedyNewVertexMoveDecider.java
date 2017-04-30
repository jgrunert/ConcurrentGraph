package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.Configuration;

public class GreedyNewVertexMoveDecider<Q extends BaseQuery> extends AbstractVertexMoveDecider<Q> {

	private static final Logger logger = LoggerFactory.getLogger(GreedyNewVertexMoveDecider.class);

	// TODO Configuration
	private static final double VerticesActiveImbalanceThreshold = Configuration.getPropertyDoubleDefault("VertexMoveActiveBalance", 0.1);
	private static final double VerticesTotalImbalanceThreshold = Configuration.getPropertyDoubleDefault("VertexMoveTotalBalance", 0.1);
	private static final long MinMoveWorkerVertices = 50;
	private static final long MinMoveTotalVertices = 500;
	private static final int MaxImproveIterations = 30;
	private static final long MaxImproveTime = Configuration.MASTER_QUERY_MOVE_CALC_TIMEOUT;



	@Override
	public VertexMoveDecision decide(Set<Integer> queryIds, Map<Integer, Map<IntSet, Integer>> workerQueryChunks,
			Map<Integer, Long> workerTotalVertices) {

		List<Integer> workerIds = new ArrayList<>(workerQueryChunks.keySet());
		Map<Integer, QueryWorkerMachine> queryMachines = new HashMap<>();
		for (Entry<Integer, Map<IntSet, Integer>> worker : workerQueryChunks.entrySet()) {
			List<QueryVertexChunk> workerChunks = new ArrayList<>();
			for (Entry<IntSet, Integer> chunk : worker.getValue().entrySet()) {
				workerChunks.add(new QueryVertexChunk(chunk.getKey(), chunk.getValue(), worker.getKey()));
			}
			QueryWorkerMachine workerMachine = new QueryWorkerMachine(workerChunks, workerTotalVertices.get(worker.getKey()));
			queryMachines.put(worker.getKey(), workerMachine);
		}

		QueryDistribution originalDistribution = new QueryDistribution(queryIds, queryMachines);
		QueryDistribution bestDistribution = originalDistribution;
		System.out.println(bestDistribution.getCurrentCosts());

		logger.info("///////////////////////////////// Move decission");
		logger.debug("Move decission queries {}", queryIds);
		logger.trace("Move decission workerQueryChunks {}", workerQueryChunks);
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

						// TODO Optimize, neglect cases.

						QueryDistribution newDistribution = bestDistribution.clone();
						int movedVerts = newDistribution.moveVertices(queryId, fromWorker, toWorker, true);

						if (movedVerts > MinMoveWorkerVertices
								&& newDistribution.getCurrentCosts() < iterBestDistribution.getCurrentCosts()
								&& checkWorkerActiveVerticesOk(bestDistribution, newDistribution, fromWorker, toWorker)
								&& checkWorkerTotalVerticesOk(bestDistribution, newDistribution, fromWorker, toWorker)) {
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
		logger.info("+++++++++++++ Stopped deciding after " + i + " iterations in " + (System.currentTimeMillis() - startTime) + "ms");

		//		bestDistribution.printMoveDistribution();
		//		bestDistribution.printMoveDecissions();

		if (totalVerticesMoved < MinMoveTotalVertices) {
			logger.info("Decided not move, not enough vertices: " + totalVerticesMoved);
			return null;
		}
		logger.info("Decided move, vertices: " + totalVerticesMoved);
		return bestDistribution.toMoveDecision(workerIds);
	}

	private static boolean checkWorkerActiveVerticesOk(QueryDistribution oldDistribution, QueryDistribution newDistribution, int fromWorker,
			int toWorker) {
		double oldFmImbalance = oldDistribution.getWorkerActiveVerticesImbalanceFactor(fromWorker);
		double oldToImbalance = oldDistribution.getWorkerActiveVerticesImbalanceFactor(toWorker);
		double newFmImbalance = newDistribution.getWorkerActiveVerticesImbalanceFactor(fromWorker);
		double newToImbalance = newDistribution.getWorkerActiveVerticesImbalanceFactor(toWorker);
		boolean newFmBalanceBetter = newFmImbalance < oldFmImbalance;
		boolean newToBalanceBetter = newToImbalance < oldToImbalance;
		boolean newFmBalanceOk = newFmImbalance < VerticesActiveImbalanceThreshold;
		boolean newToBalanceOk = newToImbalance < VerticesActiveImbalanceThreshold;
		return (newFmBalanceBetter || newFmBalanceOk) && (newToBalanceBetter || newToBalanceOk);
	}

	private static boolean checkWorkerTotalVerticesOk(QueryDistribution oldDistribution, QueryDistribution newDistribution, int fromWorker,
			int toWorker) {
		double oldFmImbalance = oldDistribution.getWorkerTotalVerticesImbalanceFactor(fromWorker);
		double oldToImbalance = oldDistribution.getWorkerTotalVerticesImbalanceFactor(toWorker);
		double newFmImbalance = newDistribution.getWorkerTotalVerticesImbalanceFactor(fromWorker);
		double newToImbalance = newDistribution.getWorkerTotalVerticesImbalanceFactor(toWorker);
		boolean newFmBalanceBetter = newFmImbalance < oldFmImbalance;
		boolean newToBalanceBetter = newToImbalance < oldToImbalance;
		boolean newFmBalanceOk = newFmImbalance < VerticesTotalImbalanceThreshold;
		boolean newToBalanceOk = newToImbalance < VerticesTotalImbalanceThreshold;
		return (newFmBalanceBetter || newFmBalanceOk) && (newToBalanceBetter || newToBalanceOk);
	}
}