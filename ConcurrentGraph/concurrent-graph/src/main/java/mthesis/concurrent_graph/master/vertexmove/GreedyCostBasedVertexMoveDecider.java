package mthesis.concurrent_graph.master.vertexmove;

import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.master.MasterQuery;

public class GreedyCostBasedVertexMoveDecider<Q extends BaseQuery> extends AbstractVertexMoveDecider<Q> {

	private long vertexBarrierMoveLastTime = System.currentTimeMillis();


	@Override
	public VertexMoveDecision decide(List<Integer> workerIds, Map<Integer, MasterQuery<Q>> activeQueries,
			Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts,
			Map<Integer, Map<Integer, Map<Integer, Integer>>> actQueryWorkerIntersects) {

		if (!Configuration.VERTEX_BARRIER_MOVE_ENABLED
				|| (System.currentTimeMillis() - vertexBarrierMoveLastTime) < Configuration.VERTEX_BARRIER_MOVE_INTERVAL)
			return null;
		vertexBarrierMoveLastTime = System.currentTimeMillis();


		QueryDistribution originalDistribution = new QueryDistribution(workerIds, actQueryWorkerActiveVerts);
		QueryDistribution bestDistribution = originalDistribution;
		System.out.println(bestDistribution.getCosts());

		System.out.println("/////////////////////////////////");
		bestDistribution.printMoveDistribution();

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

		for (int i = 0; i < 100; i++) {
			QueryDistribution iterBestDistribution = bestDistribution.clone();
			boolean anyImproves = false;

			//			System.out.println(i + " iteration");

			for (Integer queryId : activeQueries.keySet()) {
				for (Integer fromWorker : workerIds) {
					for (Integer toWorker : workerIds) {
						if (fromWorker == toWorker) continue;

						QueryDistribution newDistribution = bestDistribution.clone();
						boolean moveSuccess = newDistribution.moveVertices(queryId, fromWorker, toWorker);
						if (moveSuccess && newDistribution.getCosts() < bestDistribution.getCosts()) {
							iterBestDistribution = newDistribution;
							anyImproves = true;
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
		}

		System.out.println("+++++++++++++");
		bestDistribution.printMoveDistribution();
		System.out.println(bestDistribution.getCosts());
		bestDistribution.printMoveDecissions();

		return bestDistribution.toMoveDecision(workerIds);
	}

}
