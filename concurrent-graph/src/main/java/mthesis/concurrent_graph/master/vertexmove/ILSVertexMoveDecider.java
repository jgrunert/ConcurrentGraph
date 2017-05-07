package mthesis.concurrent_graph.master.vertexmove;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.util.MiscUtil;

public class ILSVertexMoveDecider extends AbstractVertexMoveDecider {

	private static final Logger logger = LoggerFactory.getLogger(ILSVertexMoveDecider.class);

	private final double VerticesActiveImbalanceThreshold = Configuration.getPropertyDoubleDefault("VertexMoveActiveBalance", 0.1);
	private final double VerticesActiveImbalanceMin = 1 - VerticesActiveImbalanceThreshold;
	private final double VerticesActiveImbalanceMax = 1 + VerticesActiveImbalanceThreshold;
	private final double VerticesTotalImbalanceThreshold = Configuration.getPropertyDoubleDefault("VertexMoveTotalBalance", 0.1);
	private final double VerticesTotalImbalanceMin = 1 - VerticesTotalImbalanceThreshold;
	private final double VerticesTotalImbalanceMax = 1 + VerticesTotalImbalanceThreshold;
	private final long MaxTotalImproveTime = Configuration.MASTER_QUERY_MOVE_CALC_TIMEOUT;
	private final long MaxGreedyImproveTime = Configuration.MASTER_QUERY_MOVE_CALC_TIMEOUT / 4; // TODO Config
	private final long MinMoveTotalVertices = 500; // TODO Config
	private final int MaxImproveIterations = 30; // TODO Config
	private boolean saveIlsStats = true; // TODO Config

	private long decideStartTime;

	private List<IlsLogItem> ilsLog = new ArrayList<>();
	private double latestPertubatedDistributionCosts;
	private double currentlyBestDistributionCosts;



	@Override
	public VertexMoveDecision decide(Set<Integer> queryIds, Map<Integer, Map<IntSet, Integer>> workerQueryChunks,
			Map<Integer, Long> workerTotalVertices) {
		if (queryIds.size() == 0 || workerQueryChunks.size() == 0) return null;

		List<Integer> queryIdsList = new ArrayList<>(queryIds);
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
		ilsLog.clear();
		decideStartTime = System.currentTimeMillis();


		if (saveIlsStats) {
			double costs = bestDistribution.getCurrentCosts();
			ilsLog.add(new IlsLogItem(costs, costs, costs));
			latestPertubatedDistributionCosts = costs;
			currentlyBestDistributionCosts = costs;
		}


		// Greedy improve initial distribution
		bestDistribution = optimizeGreedy(queryIds, workerIds, bestDistribution);
		if (saveIlsStats) {
			double costs = bestDistribution.getCurrentCosts();
			currentlyBestDistributionCosts = costs;
			ilsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
		}


		// Do ILS with pertubations followed by greedy optimize
		int maxIlsIteraions = 20;
		Random rd = new Random(0);
		int i = 0;
		for (; i < maxIlsIteraions && (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime; i++) {
			QueryDistribution ilsDistribution = pertubationQueryUnifyLargestPartition(queryIdsList, workerIds, bestDistribution, rd);
			latestPertubatedDistributionCosts = ilsDistribution.getCurrentCosts();
			ilsDistribution = optimizeGreedy(queryIds, workerIds, ilsDistribution);

			boolean isGoodNew = (ilsDistribution.getCurrentCosts() < bestDistribution.getCurrentCosts())
					&& checkActiveVertsOkOrBetter(bestDistribution, ilsDistribution)
					&& checkTotalVertsOkOrBetter(bestDistribution, ilsDistribution);
			if (isGoodNew) {
				bestDistribution = ilsDistribution;

				if (saveIlsStats) {
					double costs = bestDistribution.getCurrentCosts();
					currentlyBestDistributionCosts = costs;
					ilsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
				}
				logger.info("Improved pertubation " + ilsDistribution.getCurrentCosts()); // TODO Debug
			}
			if (saveIlsStats) ilsLog.add(new IlsLogItem(ilsDistribution.getCurrentCosts(), latestPertubatedDistributionCosts,
					currentlyBestDistributionCosts));
		}

		int movedVertices = bestDistribution.calculateMovedVertices();
		logger.info("+++++++++++++ Stopped deciding after " + i + " ILS iterations in " + (System.currentTimeMillis() - decideStartTime)
				+ "ms moving "
				+ movedVertices);


		// TODO Also save outcome stats
		if (saveIlsStats) {
			try (PrintWriter writer = new PrintWriter(new FileWriter("ILS_log_" + System.currentTimeMillis() + ".csv"))) {
				writer.println("Costs;CurrentlyBest;LastPertubationCosts;");
				for (IlsLogItem ilsLogItem : ilsLog) {
					writer.println(ilsLogItem.costs + ";" + ilsLogItem.currentlyBestDistributionCosts + ";"
							+ ilsLogItem.lastPertubationCosts + ";");
				}
			}
			catch (Exception e) {
				logger.error("Save ILS failed", e);
			}
		}

		if (movedVertices < MinMoveTotalVertices) {
			logger.info("Decided not move, not enough vertices: " + totalVerticesMoved);
			return null;
		}
		logger.info("Decided move, vertices: " + totalVerticesMoved);
		return bestDistribution.toMoveDecision(workerIds);
	}


	private QueryDistribution optimizeGreedy(Set<Integer> queryIds, List<Integer> workerIds, QueryDistribution baseDistribution) {
		long minActiveVertices = (long) (baseDistribution.workerActiveVertices / workerIds.size() * VerticesActiveImbalanceMin);
		long maxActiveVertices = (long) (baseDistribution.workerActiveVertices / workerIds.size() * VerticesActiveImbalanceMax);
		long minTotalVertices = (long) (baseDistribution.workerTotalVertices / workerIds.size() * VerticesTotalImbalanceMin);
		long maxTotalVertices = (long) (baseDistribution.workerTotalVertices / workerIds.size() * VerticesTotalImbalanceMax);

		QueryDistribution bestDistribution = baseDistribution;
		int i = 0;
		for (; i < MaxImproveIterations && (System.currentTimeMillis() - decideStartTime) < MaxGreedyImproveTime; i++) {
			QueryDistribution iterBestDistribution = bestDistribution.clone();
			boolean anyImproves = false;

			// TODO Move chunks instead of queries?
			for (Integer queryId : queryIds) {
				// TODO Queries not to move?

				for (Integer fromWorkerId : workerIds) {
					// Dont move from worker if worker has too few vertices afterwards
					QueryWorkerMachine fromWorker = baseDistribution.getQueryMachines().get(fromWorkerId);
					long fromWorkerQueryVertices = MiscUtil.defaultLong(fromWorker.queryVertices.get(queryId));
					if (fromWorkerQueryVertices == 0) continue;
					if (fromWorker.activeVertices - fromWorkerQueryVertices < minActiveVertices
							|| fromWorker.totalVertices - fromWorkerQueryVertices < minTotalVertices)
						continue;

					for (Integer toWorkerId : workerIds) {
						if (fromWorkerId == toWorkerId) continue;
						// Dont move to worker if has too many vertices afterwards
						QueryWorkerMachine toWorker = baseDistribution.getQueryMachines().get(toWorkerId);
						long toWorkerQueryVertices = MiscUtil.defaultLong(toWorker.queryVertices.get(queryId));
						if (toWorkerQueryVertices == 0) continue;
						if (toWorker.activeVertices + toWorkerQueryVertices > maxActiveVertices
								|| toWorker.totalVertices + toWorkerQueryVertices > maxTotalVertices)
							continue;

						// Only move smaller to larger partition
						if (fromWorkerQueryVertices > toWorkerQueryVertices) continue;

						// TODO Optimize, neglect cases.

						QueryDistribution newDistribution = bestDistribution.clone();
						// TODO Move chunks
						newDistribution.moveAllQueryVertices(queryId, fromWorkerId, toWorkerId, true);

						boolean isGoodNew = (newDistribution.getCurrentCosts() < iterBestDistribution.getCurrentCosts())
								&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution, fromWorkerId)
								&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution, toWorkerId)
								&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution, fromWorkerId)
								&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution, toWorkerId);
						if (isGoodNew) {
							if (saveIlsStats) {
								double costs = iterBestDistribution.getCurrentCosts();
								ilsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
							}
							iterBestDistribution = newDistribution;
							anyImproves = true;
						}
					}
				}
			}

			if (!anyImproves) {
				logger.info("No more improves after " + i + " " + iterBestDistribution.getCurrentCosts());
				break;
			}
			bestDistribution = iterBestDistribution;
			//totalVerticesMoved += verticesMovedIter;
		}
		logger.info("+ Stopped greedy after " + i + " iterations in " + (System.currentTimeMillis() - decideStartTime) + "ms" + " "
				+ bestDistribution.getCurrentCosts());
		return bestDistribution;
	}


	/**
	 * Pertubation by moving all partitions of a query to machine with largest partition
	 */
	private QueryDistribution pertubationQueryUnifyLargestPartition(List<Integer> queryIds, List<Integer> workerIds,
			QueryDistribution baseDistribution,
			Random rd) {
		// First unify random query
		QueryDistribution newDistribution = unifyQueryAtLargestPartition(getRandomFromList(queryIds, rd), workerIds, baseDistribution);

		// Now unify until workload balancing reached. Move smallest partition from most loaded worker to least loaded worker
		while (!workloadBalanceOk(newDistribution)) {
			while (!workloadActiveBalanceOk(newDistribution)) {
				int minLoadedId = newDistribution.getMachineMinActiveVertices();
				int maxLoadedId = newDistribution.getMachineMaxActiveVertices();
				QueryVertexChunk moveChunk = newDistribution.getQueryMachines().get(maxLoadedId).getSmallestChunk();
				//				System.out.println("A " + moveChunk + " " + maxLoadedId + "->" + minLoadedId + " "
				//						+ newDistribution.moveSingleChunkVertices(moveChunk, maxLoadedId, minLoadedId));
				newDistribution.moveSingleChunkVertices(moveChunk, maxLoadedId, minLoadedId);
			}

			while (!workloadTotalBalanceOk(newDistribution)) {
				int minLoadedId = newDistribution.getMachineMinTotalVertices();
				int maxLoadedId = newDistribution.getMachineMaxTotalVertices();
				QueryVertexChunk moveChunk = newDistribution.getQueryMachines().get(maxLoadedId).getSmallestChunk();
				//				System.out.println("T " + moveChunk + " " + maxLoadedId + "->" + minLoadedId + " "
				//						+ newDistribution.moveSingleChunkVertices(moveChunk, maxLoadedId, minLoadedId)
				//						+ "  " + newDistribution.getQueryMachines().get(minLoadedId).totalVertices + "  "
				//						+ newDistribution.getQueryMachines().get(maxLoadedId).totalVertices + " avg " + newDistribution.avgTotalVertices);
				newDistribution.moveSingleChunkVertices(moveChunk, maxLoadedId, minLoadedId);
			}

			// Workers sorted by increasing total vertices
		}

		return newDistribution;
	}

	/**
	 * Moving all partitions of a query to machine with largest partition
	 */
	private QueryDistribution unifyQueryAtLargestPartition(int pertubationQuery, List<Integer> workerIds,
			QueryDistribution baseDistribution) {
		QueryDistribution newDistribution = baseDistribution.clone();
		int bestWorkerId = 0;
		long bestWorkerPartitionSize = 0;
		for (Entry<Integer, QueryWorkerMachine> machine : baseDistribution.getQueryMachines().entrySet()) {
			long partitionSize = MiscUtil.defaultLong(machine.getValue().queryVertices.get(pertubationQuery));
			if (partitionSize > bestWorkerPartitionSize) {
				bestWorkerPartitionSize = partitionSize;
				bestWorkerId = machine.getKey();
			}
		}

		for (int worker : workerIds) {
			if (worker != bestWorkerId) {
				newDistribution.moveAllQueryVertices(pertubationQuery, worker, bestWorkerId, true);
			}
		}

		return newDistribution;
	}



	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean workloadBalanceOk(QueryDistribution distribution) {
		for (Integer worker : distribution.getQueryMachines().keySet()) {
			if (distribution.getWorkerActiveVerticesImbalanceFactor(worker) > VerticesActiveImbalanceThreshold
					|| distribution.getWorkerTotalVerticesImbalanceFactor(worker) > VerticesTotalImbalanceThreshold)
				return false;
		}
		return true;
	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean workloadActiveBalanceOk(QueryDistribution distribution) {
		for (Integer worker : distribution.getQueryMachines().keySet()) {
			if (distribution.getWorkerActiveVerticesImbalanceFactor(worker) > VerticesActiveImbalanceThreshold) return false;
		}
		return true;
	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean workloadTotalBalanceOk(QueryDistribution distribution) {
		for (Integer worker : distribution.getQueryMachines().keySet()) {
			if (distribution.getWorkerTotalVerticesImbalanceFactor(worker) > VerticesTotalImbalanceThreshold) return false;
		}
		return true;
	}


	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean checkActiveVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution) {
		for (Integer worker : oldDistribution.getQueryMachines().keySet()) {
			if (!checkActiveVertsOkOrBetter(oldDistribution, newDistribution, worker)) return false;
		}
		return true;
	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean checkTotalVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution) {
		for (Integer worker : oldDistribution.getQueryMachines().keySet()) {
			if (!checkTotalVertsOkOrBetter(oldDistribution, newDistribution, worker)) return false;
		}
		return true;
	}


	private boolean checkActiveVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution, int worker) {
		double oldImbalance = oldDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
		double newImbalance = newDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
		return (newImbalance <= oldImbalance || newImbalance <= VerticesActiveImbalanceThreshold);
	}

	private boolean checkTotalVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution, int worker) {
		double oldImbalance = oldDistribution.getWorkerTotalVerticesImbalanceFactor(worker);
		double newImbalance = newDistribution.getWorkerTotalVerticesImbalanceFactor(worker);
		return (newImbalance <= oldImbalance || newImbalance <= VerticesTotalImbalanceThreshold);
	}



	// Testing
	public static void main(String[] args) throws Exception {
		Map<String, String> overrideConfigs = new HashMap<>();
		overrideConfigs.put("MASTER_QUERY_MOVE_CALC_TIMEOUT", "999999999");
		Configuration.loadConfig("configs\\configuration.properties", overrideConfigs);

		test2Equal();
	}

	// 2 workers, 2 queries, equal hashed-like staring state
	private static void test2Equal() {
		Set<Integer> queryIds = new HashSet<>();
		Map<Integer, Map<IntSet, Integer>> workerQueryChunks = new HashMap<>();
		Map<Integer, Long> workerTotalVertices = new HashMap<>();

		for (int iQ = 0; iQ < 2; iQ++) {
			queryIds.add(iQ);
		}
		for (int iW = 0; iW < 2; iW++) {
			workerTotalVertices.put(iW, 10000L);
		}
		for (int iW = 0; iW < 2; iW++) {
			Map<IntSet, Integer> chunks = new HashMap<>();
			for (int iQ = 0; iQ < 2; iQ++) {
				chunks.put(new IntOpenHashSet(new int[] { iQ }), 5000);
			}
			workerQueryChunks.put(iW, chunks);
		}

		ILSVertexMoveDecider decider = new ILSVertexMoveDecider();
		VertexMoveDecision decission = decider.decide(queryIds, workerQueryChunks, workerTotalVertices);
		decission.printDecission();
	}

	private static <T> T getRandomFromList(List<T> list, Random rd) {
		return list.get(rd.nextInt(list.size()));
	}


	private class IlsLogItem {

		public final int costs;
		public final int lastPertubationCosts;
		public final int currentlyBestDistributionCosts;

		public IlsLogItem(double costs, double lastPertubationCosts, double currentlyBestDistributionCosts) {
			super();
			this.costs = (int) costs;
			this.lastPertubationCosts = (int) lastPertubationCosts;
			this.currentlyBestDistributionCosts = (int) currentlyBestDistributionCosts;
		}
	}
}
