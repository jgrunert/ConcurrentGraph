package mthesis.concurrent_graph.master.vertexmove;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import mthesis.concurrent_graph.util.FileUtil;
import mthesis.concurrent_graph.util.MiscUtil;

public class ILSVertexMoveDecider extends AbstractVertexMoveDecider {

	private static final Logger logger = LoggerFactory.getLogger(ILSVertexMoveDecider.class);

	private final double VertexWorkerActiveImbalance = Configuration.getPropertyDoubleDefault("VertexMoveActiveImbalance", 0.4);
	private final double VertexWorkerTotalImbalance = Configuration.getPropertyDoubleDefault("VertexMoveTotalImbalance", 0.1);
	private final double VertexAvgActiveImbalance = VertexWorkerActiveImbalance;//TODO Config
	private final double VertexAvgTotalImbalance = VertexWorkerTotalImbalance;//TODO Config
	private final long MaxTotalImproveTime = Configuration.MASTER_QUERY_MOVE_CALC_TIMEOUT;
	private final long MaxGreedyImproveTime = Configuration.getPropertyLongDefault("VertexMoveMaxGreedyTime", 500);
	private final long MinMoveTotalVertices = Configuration.getPropertyLongDefault("VertexMoveMinMoveVertices", 500);
	private final int MaxILSIterations = Configuration.getPropertyIntDefault("VertexMoveMaxILSIterations", 20);
	private final int MaxGreedyIterations = Configuration.getPropertyIntDefault("VertexMoveMaxGreedyIterations", 100);
	private final double QueryKeepLocalThreshold = Configuration.getPropertyDoubleDefault("QueryKeepLocalThreshold", 0.5);
	private boolean saveIlsStats = Configuration.getPropertyBoolDefault("SaveIlsStats", false);

	private long decideStartTime;
	private int ilsRunNumber = 0;

	private static final String ilsLogDir = "output" + File.separator + "stats" + File.separator + "ils";
	private List<IlsLogItem> ilsStepsLog = new ArrayList<>();
	private PrintWriter ilsLogWriter;
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

		logger.info("///////////////////////////////// Move decission " + ilsRunNumber);
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
		ilsStepsLog.clear();
		decideStartTime = System.currentTimeMillis();


		if (saveIlsStats) {
			if (ilsRunNumber == 0)
				FileUtil.createDirOrEmptyFiles("output" + File.separator + "stats" + File.separator + "ils");

			double costs = bestDistribution.getCurrentCosts();
			ilsStepsLog.add(new IlsLogItem(costs, costs, costs));
			latestPertubatedDistributionCosts = costs;
			currentlyBestDistributionCosts = costs;

			try {
				ilsLogWriter = new PrintWriter(new FileWriter(ilsLogDir + File.separator + ilsRunNumber + "_log.txt"));
			}
			catch (IOException e) {
				logger.error("", e);
				return null;
			}

			ilsLogWriter.println("Initial costs\t" + bestDistribution.getCurrentCosts());
			ilsLogWriter.print("workerVertices\t");
			for (int worker : workerIds) {
				ilsLogWriter.print(worker + ": "
						+ bestDistribution.getQueryMachines().get(worker).activeVertices + "/"
						+ bestDistribution.getQueryMachines().get(worker).totalVertices + " ");
			}
			ilsLogWriter.println();
		}


		// Greedy improve initial distribution
		bestDistribution = optimizeGreedy(queryIds, workerIds, bestDistribution);
		if (saveIlsStats) {
			double costs = bestDistribution.getCurrentCosts();
			currentlyBestDistributionCosts = costs;
			ilsStepsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
			ilsLogWriter.println("After greedy\t" + costs + "\t" + (System.currentTimeMillis() - decideStartTime) + "ms");
		}


		// Do ILS with pertubations followed by greedy optimize
		Random rd = new Random(0);
		int i = 0;
		for (; i < MaxILSIterations && (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime; i++) {
			// Pertubation
			QueryDistribution ilsDistribution = pertubationQueryUnifyLargestPartition(queryIdsList, workerIds, bestDistribution, rd);
			latestPertubatedDistributionCosts = ilsDistribution.getCurrentCosts();
			if (saveIlsStats) {
				ilsLogWriter.println();
				ilsLogWriter.println(
						"pertubation run \t" + i + "\t" + latestPertubatedDistributionCosts + "\t"
								+ (System.currentTimeMillis() - decideStartTime) + "ms");
			}

			// Greedy after pertubation
			ilsDistribution = optimizeGreedy(queryIds, workerIds, ilsDistribution);

			// Check if result has better costs and valid
			boolean isGoodNew = (ilsDistribution.getCurrentCosts() < bestDistribution.getCurrentCosts())
					&& checkActiveVertsOkOrBetter(bestDistribution, ilsDistribution)
					&& checkTotalVertsOkOrBetter(bestDistribution, ilsDistribution);
			if (isGoodNew) {
				bestDistribution = ilsDistribution;

				if (saveIlsStats) {
					double costs = bestDistribution.getCurrentCosts();
					currentlyBestDistributionCosts = costs;
					ilsStepsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
					ilsLogWriter.println(
							"better greedy run\t" + i + "\t" + ilsDistribution.getCurrentCosts() + "\t"
									+ (System.currentTimeMillis() - decideStartTime)
									+ "ms");
				}
				logger.debug("Improved pertubation\t" + ilsDistribution.getCurrentCosts());
			}
			else {
				if (saveIlsStats) {
					ilsLogWriter.println(
							"discard greedy run\t" + i + "\t" + ilsDistribution.getCurrentCosts() + "\t"
									+ (System.currentTimeMillis() - decideStartTime)
									+ "ms");
				}
			}
			if (saveIlsStats) ilsStepsLog.add(new IlsLogItem(ilsDistribution.getCurrentCosts(), latestPertubatedDistributionCosts,
					currentlyBestDistributionCosts));
		}

		int movedVertices = bestDistribution.calculateMovedVertices();
		logger.info("+++++++++++++ Stopped deciding after " + i + " ILS iterations in " + (System.currentTimeMillis() - decideStartTime)
				+ "ms moving "
				+ movedVertices);


		if (saveIlsStats) {
			try (PrintWriter writer = new PrintWriter(new FileWriter(ilsLogDir + File.separator + ilsRunNumber + "_steps.csv"))) {
				writer.println("Costs;CurrentlyBest;LastPertubationCosts;");
				for (IlsLogItem ilsLogItem : ilsStepsLog) {
					writer.println(ilsLogItem.costs + ";" + ilsLogItem.currentlyBestDistributionCosts + ";"
							+ ilsLogItem.lastPertubationCosts + ";");
				}
				ilsLogWriter.close();
			}
			catch (Exception e) {
				logger.error("Save ILS failed", e);
			}
		}
		ilsRunNumber++;

		if (movedVertices < MinMoveTotalVertices) {
			logger.info("Decided not move, not enough vertices: " + totalVerticesMoved);
			return null;
		}
		logger.info("Decided move, vertices: " + totalVerticesMoved);
		return bestDistribution.toMoveDecision(workerIds);
	}


	private QueryDistribution optimizeGreedy(Set<Integer> queryIds, List<Integer> workerIds, QueryDistribution baseDistribution) {
		long minActiveVertices = (long) (baseDistribution.workerActiveVertices / workerIds.size() * (1.0 - VertexWorkerActiveImbalance));
		long maxActiveVertices = Math.min(
				(long) (baseDistribution.workerActiveVertices / workerIds.size() / (1.0 - VertexWorkerActiveImbalance)),
				Long.MAX_VALUE / 4);
		long minTotalVertices = (long) (baseDistribution.workerTotalVertices / workerIds.size() * (1.0 - VertexWorkerActiveImbalance));
		long maxTotalVertices = Math.min(
				(long) (baseDistribution.workerTotalVertices / workerIds.size() / (1.0 - VertexWorkerTotalImbalance)),
				Long.MAX_VALUE / 4);

		QueryDistribution bestDistribution = baseDistribution.clone();

		// Force local queries
		//		Map<Integer, Double> sortedQueryLocalities = queryLocalities.entrySet().stream()
		//				.sorted(Entry.comparingByValue())
		//				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
		//						(e1, e2) -> e1, LinkedHashMap::new));
		//		for (Entry<Integer, Double> query : sortedQueryLocalities.entrySet()) {
		//			if (query.getValue() > QueryForceLocalThreshold) {
		//				int targetMachine = bestDistribution.getMachineMaxQueryVertices(query.getKey());
		//				QueryDistribution newDistribution = unifyQueryAtWorker(query.getKey(), workerIds, targetMachine, bestDistribution);
		//
		//				boolean isGoodNew = (newDistribution.getCurrentCosts() < bestDistribution.getCurrentCosts())
		//						&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution)
		//						&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution)
		//						&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution)
		//						&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution);
		//				if (query.getValue() >= 1.0 || isGoodNew) {
		//					if (saveIlsStats) {
		//						double costs = newDistribution.getCurrentCosts();
		//						ilsStepsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
		//					}
		//					bestDistribution = newDistribution;
		//					if (saveIlsStats) {
		//						ilsLogWriter.println("Do Force query local\t" + query.getKey() + "\t" + query.getValue() + "\t" + targetMachine);
		//					}
		//				}
		//				else {
		//					if (saveIlsStats) {
		//						ilsLogWriter.println("No Force query local\t" + query.getKey() + "\t" + query.getValue() + "\t" + targetMachine);
		//					}
		//				}
		//
		//			}
		//		}

		// Get query localities
		Map<Integer, Double> queryLocalities = bestDistribution.getQueryLoclities();
		Map<Integer, Integer> queryLargestPartitions = bestDistribution.getQueryLargestPartitions();
		if (saveIlsStats) {
			ilsLogWriter.println("queryVertices\t" + bestDistribution.queryVertices);
			ilsLogWriter.println("queryLocalities\t" + queryLocalities);
			ilsLogWriter.println("queryLargestPartitions\t" + queryLargestPartitions);
		}

		// Find non-keep-local-queries that can be moved
		queryLocalities = bestDistribution.getQueryLoclities();
		if (saveIlsStats) {
			ilsLogWriter.println("queryLocalities2\t" + queryLocalities);
		}
		Map<Integer, Integer> localQueries = new HashMap<>(); // Queries with high locality (key) and their largest partition (value)
		IntSet nonlocalQueries = new IntOpenHashSet();
		for (Entry<Integer, Double> query : queryLocalities.entrySet()) {
			if (query.getValue() > QueryKeepLocalThreshold) {
				localQueries.put(query.getKey(), queryLargestPartitions.get(query.getKey()));
			}
			else {
				nonlocalQueries.add(query.getKey());
			}
		}
		if (saveIlsStats) {
			ilsLogWriter.println("nonlocalQueries\t" + nonlocalQueries);
			ilsLogWriter.println("localQueries\t" + localQueries);
			ilsLogWriter.print("workerBalances\t");
			for (int worker : workerIds) {
				ilsLogWriter.print(worker + ": "
						+ (double) ((int) (baseDistribution.getWorkerActiveVerticesImbalanceFactor(worker) * 100)) / 100 + "/"
						+ (double) ((int) (baseDistribution.getWorkerTotalVerticesImbalanceFactor(worker) * 100)) / 100 + " ");
			}
			ilsLogWriter.println();
		}


		// Greedy iterative move queries
		int i = 0;
		long greedyStartTime = System.currentTimeMillis();
		for (; i < MaxGreedyIterations && (System.currentTimeMillis() - greedyStartTime) < MaxGreedyImproveTime
				&& (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime; i++) {
			QueryDistribution iterBestDistribution = bestDistribution.clone();
			boolean anyImproves = false;

			for (Integer queryId : queryIds) {
				Integer queryLocality = localQueries.get(queryId);

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

						// Only move local queries to their largest partition
						if (queryLocality != null && !queryLocality.equals(toWorkerId)) continue;

						// Only move smaller to larger partition
						if (fromWorkerQueryVertices > toWorkerQueryVertices) continue;

						// TODO Optimize, neglect cases.

						QueryDistribution newDistribution = bestDistribution.clone();
						// TODO Move chunks
						newDistribution.moveAllQueryVertices(queryId, fromWorkerId, toWorkerId, localQueries);

						boolean isGoodNew = (newDistribution.getCurrentCosts() < iterBestDistribution.getCurrentCosts())
								&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution)
								&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution);
						if (isGoodNew) {
							if (saveIlsStats) {
								double costs = iterBestDistribution.getCurrentCosts();
								ilsStepsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
							}
							iterBestDistribution = newDistribution;
							anyImproves = true;
						}
					}
				}
			}

			if (!anyImproves) {
				if (saveIlsStats) {
					ilsLogWriter.println("No more improves after " + i + " " + iterBestDistribution.getCurrentCosts());
				}
				break;
			}
			bestDistribution = iterBestDistribution;
			//totalVerticesMoved += verticesMovedIter;
		}

		// TODO Move chunks instead of queries?
		//		int i = 0;
		//		for (; i < MaxImproveIterations && (System.currentTimeMillis() - decideStartTime) < MaxGreedyImproveTime; i++) {
		//			QueryDistribution iterBestDistribution = bestDistribution.clone();
		//			boolean anyImproves = false;
		//
		//			for (Integer fromWorkerId : workerIds) {
		//				// TODO Soft limits for worker balance?
		//
		//				QueryWorkerMachine fromWorker = baseDistribution.getQueryMachines().get(fromWorkerId);
		//				for (QueryVertexChunk chunk : fromWorker.queryChunks) {
		//					// Dont move from worker if worker has too few vertices afterwards
		//					if (chunk.numVertices == 0) continue;
		//					if (fromWorker.activeVertices - chunk.numVertices < minActiveVertices
		//							|| fromWorker.totalVertices - chunk.numVertices < minTotalVertices) {
		//						continue;
		//					}
		//
		//					// Dont move chunk if contains immovable query
		//					boolean nonMovable = false;
		//					for (Integer chunkQuery : chunk.queries) {
		//						if (nonMovableQueries.contains(chunkQuery)) {
		//							nonMovable = true;
		//							break;
		//						}
		//					}
		//					if (nonMovable) {
		//						continue;
		//					}
		//
		//
		//					for (Integer toWorkerId : workerIds) {
		//						if (fromWorker.equals(toWorkerId)) continue;
		//
		//						// TODO Soft limits for worker balance?
		//
		//						// Dont move to worker if has too many vertices afterwards
		//						QueryWorkerMachine toWorker = baseDistribution.getQueryMachines().get(toWorkerId);
		//						if (chunk.numVertices == 0) continue;
		//						if (toWorker.activeVertices + chunk.numVertices > maxActiveVertices
		//								|| toWorker.totalVertices + chunk.numVertices > maxTotalVertices) {
		//							continue;
		//						}
		//
		//						// Dont move if has more common vertices on src machine
		//						if (fromWorker.getNumVerticesWithQueries(chunk.queries) > toWorker.getNumVerticesWithQueries(chunk.queries)) {
		//							continue;
		//						}
		//
		//						// Try move
		//						QueryDistribution newDistribution = bestDistribution.clone();
		//						newDistribution.moveSingleChunkVertices(chunk, fromWorkerId, toWorkerId);
		//
		//						boolean isGoodNew = (newDistribution.getCurrentCosts() < iterBestDistribution.getCurrentCosts())
		//								&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution, fromWorkerId)
		//								&& checkActiveVertsOkOrBetter(bestDistribution, newDistribution, toWorkerId)
		//								&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution, fromWorkerId)
		//								&& checkTotalVertsOkOrBetter(bestDistribution, newDistribution, toWorkerId);
		//						if (isGoodNew) {
		//							if (saveIlsStats) {
		//								double costs = iterBestDistribution.getCurrentCosts();
		//								ilsStepsLog.add(new IlsLogItem(costs, latestPertubatedDistributionCosts, currentlyBestDistributionCosts));
		//							}
		//							iterBestDistribution = newDistribution;
		//							anyImproves = true;
		//						}
		//					}
		//				}
		//			}
		//
		//			if (!anyImproves) {
		//				if (saveIlsStats) {
		//					ilsLogWriter.println("No more improves after\t" + i + " " + iterBestDistribution.getCurrentCosts());
		//				}
		//				logger.info("No more improves after " + i + " " + iterBestDistribution.getCurrentCosts());
		//				break;
		//			}
		//			bestDistribution = iterBestDistribution;
		//			//totalVerticesMoved += verticesMovedIter;
		//		}
		//		if (saveIlsStats) {
		//			ilsLogWriter.println("Finished greedy after\t" + i + " " + bestDistribution.getCurrentCosts());
		//		}
		//		logger.info("+ Stopped greedy after " + i + " iterations in " + (System.currentTimeMillis() - decideStartTime) + "ms" + " "
		//				+ bestDistribution.getCurrentCosts());
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
		while (!(workloadActiveBalanceOk(newDistribution) && workloadTotalBalanceOk(newDistribution))
				&& (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime) {
			while (!workloadActiveBalanceOk(newDistribution) && (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime) {
				int minLoadedId = newDistribution.getMachineMinActiveVertices();
				int maxLoadedId = newDistribution.getMachineMaxActiveVertices();
				QueryVertexChunk moveChunk = newDistribution.getQueryMachines().get(maxLoadedId).getSmallestChunk();
				//				System.out.println("A " + moveChunk + " " + maxLoadedId + "->" + minLoadedId + " "
				//						+ newDistribution.moveSingleChunkVertices(moveChunk, maxLoadedId, minLoadedId));
				newDistribution.moveSingleChunkVertices(moveChunk, maxLoadedId, minLoadedId);
			}

			while (!workloadTotalBalanceOk(newDistribution) && (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime) {
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
		int bestWorkerId = 0;
		long bestWorkerPartitionSize = 0;
		for (Entry<Integer, QueryWorkerMachine> machine : baseDistribution.getQueryMachines().entrySet()) {
			long partitionSize = MiscUtil.defaultLong(machine.getValue().queryVertices.get(pertubationQuery));
			if (partitionSize > bestWorkerPartitionSize) {
				bestWorkerPartitionSize = partitionSize;
				bestWorkerId = machine.getKey();
			}
		}

		if (saveIlsStats) {
			ilsLogWriter.println(
					"unifyQueryAtLargestPartition " + pertubationQuery + " at " + bestWorkerId + " " + bestWorkerPartitionSize + "/"
							+ baseDistribution.queryVertices.get(pertubationQuery));
		}

		return unifyQueryAtWorker(pertubationQuery, workerIds, bestWorkerId, baseDistribution);
	}

	/**
	 * Moving all partitions of a query to machine with largest partition
	 */
	private QueryDistribution unifyQueryAtWorker(int pertubationQuery, List<Integer> workerIds, int targetWorkerId,
			QueryDistribution baseDistribution) {
		QueryDistribution newDistribution = baseDistribution.clone();
		for (int worker : workerIds) {
			if (worker != targetWorkerId) {
				newDistribution.moveAllQueryVertices(pertubationQuery, worker, targetWorkerId, true);
			}
		}
		return newDistribution;
	}



	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean workloadActiveBalanceOk(QueryDistribution distribution) {
		Map<Integer, QueryWorkerMachine> workers = distribution.getQueryMachines();
		double avgImbalance = 0.0;
		for (Integer worker : workers.keySet()) {
			double imb = distribution.getWorkerActiveVerticesImbalanceFactor(worker);
			if (imb > VertexWorkerActiveImbalance) return false;
			avgImbalance += imb;
		}
		return (avgImbalance / workers.size()) <= VertexAvgActiveImbalance;
	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean workloadTotalBalanceOk(QueryDistribution distribution) {
		Map<Integer, QueryWorkerMachine> workers = distribution.getQueryMachines();
		double avgImbalance = 0.0;
		for (Integer worker : workers.keySet()) {
			double imb = distribution.getWorkerTotalVerticesImbalanceFactor(worker);
			if (imb > VertexWorkerTotalImbalance) return false;
			avgImbalance += imb;
		}
		return (avgImbalance / workers.size()) <= VertexAvgTotalImbalance;
	}


	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean checkActiveVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution) {
		Map<Integer, QueryWorkerMachine> workers = oldDistribution.getQueryMachines();
		double oldAvgImbalance = 0.0;
		double newAvgImbalance = 0.0;
		for (Integer worker : workers.keySet()) {
			double oldImb = oldDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
			double newImb = newDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
			if (newImb > VertexWorkerActiveImbalance && newImb > oldImb) return false;
			oldAvgImbalance += oldImb;
			newAvgImbalance += newImb;
		}
		return (newAvgImbalance / workers.size()) <= VertexAvgActiveImbalance || newAvgImbalance <= oldAvgImbalance;
	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean checkTotalVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution) {
		Map<Integer, QueryWorkerMachine> workers = oldDistribution.getQueryMachines();
		double oldAvgImbalance = 0.0;
		double newAvgImbalance = 0.0;
		for (Integer worker : workers.keySet()) {
			double oldImb = oldDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
			double newImb = newDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
			if (newImb > VertexWorkerTotalImbalance && newImb > oldImb) return false;
			oldAvgImbalance += oldImb;
			newAvgImbalance += newImb;
		}
		return (newAvgImbalance / workers.size()) <= VertexAvgTotalImbalance || newAvgImbalance <= oldAvgImbalance;
	}


	//	private boolean checkActiveVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution, int worker) {
	//		double oldImbalance = oldDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
	//		double newImbalance = newDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
	//		return (newImbalance <= oldImbalance || newImbalance <= VertexWorkerActiveImbalance);
	//	}
	//
	//	private boolean checkTotalVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution, int worker) {
	//		double oldImbalance = oldDistribution.getWorkerTotalVerticesImbalanceFactor(worker);
	//		double newImbalance = newDistribution.getWorkerTotalVerticesImbalanceFactor(worker);
	//		return (newImbalance <= oldImbalance || newImbalance <= VertexWorkerTotalImbalance);
	//	}



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
