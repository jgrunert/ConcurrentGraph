package mthesis.concurrent_graph.master.vertexmove;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

	//	private final double VertexWorkerActiveImbalance = Configuration.getPropertyDoubleDefault("VertexMoveActiveImbalance", 0.6);
	private final double VertexWorkerTotalImbalance = Configuration.getPropertyDoubleDefault("VertexMoveTotalImbalance", 0.3);
	//	private final double VertexAvgActiveImbalance = Configuration.getPropertyDoubleDefault("VertexAvgActiveImbalance", 0.4);
	private final double VertexAvgTotalImbalance = Configuration.getPropertyDoubleDefault("VertexAvgTotalImbalance", 0.2);
	private final long MaxTotalImproveTime = Configuration.MASTER_QUERY_MOVE_CALC_TIMEOUT;
	private final long MaxGreedyImproveTime = Configuration.getPropertyLongDefault("VertexMoveMaxGreedyTime", 500);
	private final long MinMoveTotalVertices = Configuration.getPropertyLongDefault("VertexMoveMinMoveVertices", 500);
	private final int MaxILSIterations = Configuration.getPropertyIntDefault("VertexMoveMaxILSIterations", 20);
	private final int MaxGreedyIterations = Configuration.getPropertyIntDefault("VertexMoveMaxGreedyIterations", 100);
	private final double QueryKeepLocalThreshold = Configuration.getPropertyDoubleDefault("QueryKeepLocalThreshold", 0.5);
	private boolean saveIlsStats = Configuration.getPropertyBoolDefault("SaveIlsStats", false);
	private final int ClustersPerWorker = Configuration.getPropertyIntDefault("ClustersPerWorker", 4);
	private final int ClustersAdditional = Configuration.getPropertyIntDefault("ClustersAdditional", 0);
	private final boolean IlsBalanceFirst = Configuration.getPropertyBoolDefault("IlsBalanceFirst", true);

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
		Collections.sort(queryIdsList);
		List<Integer> workerIds = new ArrayList<>(workerQueryChunks.keySet());
		Map<Integer, QueryWorkerMachine> queryMachines = new HashMap<>();
		for (Entry<Integer, Map<IntSet, Integer>> worker : workerQueryChunks.entrySet()) {
			List<QueryVertexChunk> workerChunks = new ArrayList<>();
			for (Entry<IntSet, Integer> chunk : worker.getValue().entrySet()) {
				workerChunks.add(new QueryVertexChunk(chunk.getKey(), chunk.getValue(), worker.getKey()));
			}
			QueryWorkerMachine workerMachine = new QueryWorkerMachine(worker.getKey(), workerChunks,
					workerTotalVertices.get(worker.getKey()));
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
			latestPertubatedDistributionCosts = costs;
			currentlyBestDistributionCosts = costs;
			logIlsStep(bestDistribution);

			try {
				ilsLogWriter = new PrintWriter(new FileWriter(ilsLogDir + File.separator + ilsRunNumber + "_log.txt"));
			}
			catch (IOException e) {
				logger.error("", e);
				return null;
			}

			printIlsLog("Initial costs\t" + bestDistribution.getCurrentCosts());
			printIlsLog("Initial imbalance\t" + bestDistribution.getAverageTotalVerticesImbalanceFactor());
			String msg = ("workerVertices\t");
			for (int worker : workerIds) {
				msg += (worker + ": "
						+ bestDistribution.getQueryMachines().get(worker).chunkVertices + " "
						+ bestDistribution.getQueryMachines().get(worker).totalVertices + " ");
			}
			printIlsLog(msg);

			// Warning: Can be very long log. Consider disabling for large setups
			printIlsLog("query partitions: ");
			for (Entry<Integer, Map<IntSet, Integer>> workerChunks : workerQueryChunks.entrySet()) {
				ilsLogWriter.println("  " + workerChunks.getKey() + ": " + workerChunks.getValue());
			}
			printIlsLog("worker query vertices: ");
			for (Entry<Integer, QueryWorkerMachine> worker : originalDistribution.getQueryMachines().entrySet()) {
				ilsLogWriter.println("  " + worker.getKey() + ": " + worker.getValue().queryVertices);
			}
		}



		// Calculate query intersects
		// ClusterId->(Queries,Intersects)
		Map<Integer, Integer> queryClusterIds = new HashMap<>();
		Map<Integer, QueryCluster> clusters = new LinkedHashMap<>();
		for (Integer queryId : queryIds) {
			Map<Integer, Integer> intersects = new HashMap<>();

			for (Entry<Integer, Map<IntSet, Integer>> worker : workerQueryChunks.entrySet()) {
				for (Entry<IntSet, Integer> chunk : worker.getValue().entrySet()) {
					if (chunk.getKey().contains(queryId)) {
						for (Integer chunkQuery : chunk.getKey()) {
							if (chunkQuery.equals(queryId)) continue;
							intersects.put(chunkQuery, MiscUtil.defaultInt(intersects.get(chunkQuery)) + chunk.getValue());
						}
					}
				}
			}

			List<Integer> cluster = new ArrayList<>();
			cluster.add(queryId);
			clusters.put(queryId, new QueryCluster(queryId, bestDistribution.queryVertices.get(queryId), intersects));
			queryClusterIds.put(queryId, queryId);
		}
		if (saveIlsStats) {
			printIlsLog("initial cluster intersects: ");
			for (QueryCluster qI : clusters.values()) {
				ilsLogWriter.println("\t" + qI + ": " + qI.intersects);
			}
		}


		// Clustering/merging
		//		Map<Pair<Integer, Integer>, Double> clusterIntersectPairs = new HashMap<>();
		//		for (Entry<Integer, Pair<List<Integer>, Map<Integer, Integer>>> cluster : queryClusterIntersects.entrySet()) {
		//			for (Entry<Integer, Integer> intersect : cluster.getValue().second.entrySet()) {
		//				int intersectSize = intersect.getValue();
		//				int q1 = cluster.getKey();
		//				int q2 = intersect.getKey();
		//				long queriesSize = (bestDistribution.queryVertices.get(q1) + bestDistribution.queryVertices.get(q2)) / 2;
		//				clusterIntersectPairs.put(new Pair<>(q1, q2), (double) intersectSize / queriesSize);
		//			}
		//		}
		//		LinkedHashMap<Pair<Integer, Integer>, Double> clusterIntersectsPairsSorted = clusterIntersectPairs.entrySet().stream()
		//				.sorted(Map.Entry.<Pair<Integer, Integer>, Double>comparingByValue().reversed())
		//				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
		//						(e1, e2) -> e1, LinkedHashMap::new));
		//		if (saveIlsStats) {
		//			printIlsLog("queryClusterIntersects: " + queryClusterIntersects);
		//			printIlsLog("clusterIntersectsSorted: " + clusterIntersectsPairsSorted);
		//		}



		printIlsLog("Start clustering");
		//		Random rdCluster = new Random(0);

		int clusterCount = ClustersAdditional + workerIds.size() * ClustersPerWorker; // TODO Config
		//		for (Entry<Pair<Integer, Integer>, Double> mergeIntersect : clusterIntersectsPairsSorted.entrySet()) {
		//			if (queryClusterIntersects.size() <= clusterCount) break;
		while (clusters.size() > clusterCount) {
			//		while (true) {

			//			printIlsLog("clusters: ");
			//			for (QueryCluster c : clusters.values()) {
			//				ilsLogWriter.println("\t" + c + "\t" + c.vertices + "\t" + c.intersects);
			//			}

			// Merge largest cluster intersect
			Integer clusterIdA = 0;
			Integer clusterIdB = 0;
			double bestIntersect = 0;
			for (QueryCluster c0 : clusters.values()) {
				for (Entry<Integer, Integer> cIntersect : c0.intersects.entrySet()) {
					long c1Verts = clusters.get(cIntersect.getKey()).vertices;
					double intersect = (double) cIntersect.getValue() / ((c0.vertices + c1Verts) / 2);
					//double intersect = (double) cIntersect.getValue() / c0.vertices;
					//double intersect = (double) cIntersect.getValue() / (Math.min(c0.vertices, c1Verts));
					if (intersect > bestIntersect) {
						bestIntersect = intersect;
						clusterIdA = c0.id;
						clusterIdB = cIntersect.getKey();
					}
				}
			}

			// Terminate if no more intersects
			//			if (bestIntersect < 0.5) {
			if (bestIntersect < 0.2) {// TODO Config
				//								if (bestIntersect <= 0) {
				printIlsLog("No more relevant intersections, terminate with more clusters: " + clusters.size());
				break;
			}

			// Merge largest intersect
			//			int largestIntersectA = mergeIntersect.getKey().first;
			//			int largestIntersectB = mergeIntersect.getKey().second;
			//			Integer clusterIdA = queryClusters.get(largestIntersectA);
			//			Integer clusterIdB = queryClusters.get(largestIntersectB);

			// Merge random
			//			List<Integer> clusterIds = new ArrayList<>(queryClusterIntersects.keySet());
			//			Integer clusterIdA = clusterIds.get(rdCluster.nextInt(clusterIds.size()));
			//			List<Integer> clusterOtherIntersects = new ArrayList<>(queryClusterIntersects.get(clusterIdA).second.keySet());
			//			if (clusterOtherIntersects.size() == 0) continue;
			//			Integer clusterIdB = clusterOtherIntersects.get(rdCluster.nextInt(clusterOtherIntersects.size()));
			//			Map<Integer, Integer> clusterAIntersects = queryClusterIntersects.get(clusterIdA).second;
			//			int largestIntersectTmp = 0;
			//			for (Entry<Integer, Integer> intersect : clusterAIntersects.entrySet()) {
			//				if (intersect.getValue() > largestIntersectTmp) {
			//					largestIntersectTmp = intersect.getValue();
			//					clusterIdB = intersect.getKey();
			//				}
			//			}



			if (clusterIdA == clusterIdB) {
				continue;
			}

			//			for (Entry<Integer, Pair<List<Integer>, Map<Integer, Integer>>> cluster : queryClusterIntersects.entrySet()) {
			//				for (Entry<Integer, Integer> intersect : cluster.getValue().second.entrySet()) {
			//					if (intersect.getValue() > largestIntersect) {
			//						largestIntersect = intersect.getValue();
			//						largestIntersectFrom = cluster.getKey();
			//						largestIntersectTo = intersect.getKey();
			//					}
			//				}
			//			}

			if (saveIlsStats) {
				printIlsLog("merge: " + clusterIdA + " " + clusterIdB + " intersecting " + bestIntersect);
			}

			clusters.get(clusterIdA).mergeOtherCluster(clusters.get(clusterIdB), queryClusterIds, clusters);
		}
		printIlsLog("Finished clustering");


		List<Integer> clusterIds = new ArrayList<>(clusters.keySet());
		Collections.sort(clusterIds);
		if (saveIlsStats) {
			//			printIlsLog("clusters: ");
			//			for (QueryCluster c : clusters.values()) {
			//				ilsLogWriter.println("\t" + qI.getKey() + ": " + qI.getValue().);
			//			}
			//			printIlsLog("query clusters: ");
			//			for (Entry<Integer, Integer> qI : queryClusters.entrySet()) {
			//				ilsLogWriter.println("\t" + qI);
			//			}
			printIlsLog("clusters: ");
			for (QueryCluster c : clusters.values()) {
				ilsLogWriter.println("\t" + c + "\t" + c.vertices + "\t" + c.intersects);
			}
		}


		// Assign chunks to clusters
		for (Entry<Integer, QueryWorkerMachine> worker : bestDistribution.getQueryMachines().entrySet()) {
			for (QueryVertexChunk chunk : worker.getValue().queryChunks) {
				for (int chunkQuery : chunk.queries) {
					chunk.clusters.add(queryClusterIds.get(chunkQuery));
				}
			}
		}

		printIlsLog("worker query vertices: ");
		for (Entry<Integer, QueryWorkerMachine> worker : originalDistribution.getQueryMachines().entrySet()) {
			ilsLogWriter.println("  " + worker.getKey() + ": " + worker.getValue().totalVertices + "/" + worker.getValue().chunkVertices
					+ " " + worker.getValue().queryVertices);
		}
		printIlsLog("worker cluster vertices: ");
		for (Entry<Integer, QueryWorkerMachine> worker : originalDistribution.getQueryMachines().entrySet()) {
			ilsLogWriter.println("  " + worker.getKey() + ": " + worker.getValue().totalVertices + "/" + worker.getValue().chunkVertices
					+ " " + worker.getValue().getClusterVertices());
		}


		// Initial balancing
		printIlsLog("Initial balance: " + workloadTotalBalanceOk(bestDistribution) + " "
				+ bestDistribution.getAverageTotalVerticesImbalanceFactor());
		if (IlsBalanceFirst) {
			printIlsLog("Start IlsBalanceFirst");
			bestDistribution = balanceDistribution(bestDistribution);
			printIlsLog(
					"Finished IlsBalanceFirst: " + workloadTotalBalanceOk(bestDistribution) + " "
							+ bestDistribution.getAverageTotalVerticesImbalanceFactor());

			printIlsLog("post balance worker query vertices: ");
			for (Entry<Integer, QueryWorkerMachine> worker : originalDistribution.getQueryMachines().entrySet()) {
				ilsLogWriter.println("  " + worker.getKey() + ": " + worker.getValue().totalVertices + "/" + worker.getValue().chunkVertices
						+ " " + worker.getValue().queryVertices);
			}
			printIlsLog("post balance worker cluster vertices: ");
			for (Entry<Integer, QueryWorkerMachine> worker : originalDistribution.getQueryMachines().entrySet()) {
				ilsLogWriter.println("  " + worker.getKey() + ": " + worker.getValue().totalVertices + "/" + worker.getValue().chunkVertices
						+ " " + worker.getValue().getClusterVertices());
			}
		}



		// Greedy improve initial distribution
		bestDistribution = optimizeGreedy(queryIds, workerIds, bestDistribution, clusterIds);
		if (saveIlsStats) {
			double costs = bestDistribution.getCurrentCosts();
			currentlyBestDistributionCosts = costs;
			logIlsStep(bestDistribution);
			printIlsLog("After greedy\t" + costs + "\t" + (System.currentTimeMillis() - decideStartTime) + "ms");
		}


		// Do ILS with pertubations followed by greedy optimize
		Random rd = new Random(ilsRunNumber);
		int i = 0;
		for (; i < MaxILSIterations && (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime; i++) {
			// Pertubation
			QueryDistribution ilsDistribution = pertubationClusterUnifyLargestPartition(queryIdsList, workerIds, bestDistribution, rd);
			latestPertubatedDistributionCosts = ilsDistribution.getCurrentCosts();
			if (saveIlsStats) {
				ilsLogWriter.println();
				printIlsLog(
						"pertubation run \t" + i + "\t" + latestPertubatedDistributionCosts + "\t"
								+ (System.currentTimeMillis() - decideStartTime) + "ms");
			}

			// Greedy after pertubation
			ilsDistribution = optimizeGreedy(queryIds, workerIds, ilsDistribution, clusterIds);

			// Check if result has better costs and valid
			boolean isGoodNew = (ilsDistribution.getCurrentCosts() < bestDistribution.getCurrentCosts())
					//					&& checkActiveVertsOkOrBetter(bestDistribution, ilsDistribution)
					&& checkTotalVertsOkOrBetter(bestDistribution, ilsDistribution);
			if (isGoodNew) {
				bestDistribution = ilsDistribution;

				if (saveIlsStats) {
					currentlyBestDistributionCosts = bestDistribution.getCurrentCosts();
					logIlsStep(bestDistribution);
					printIlsLog(
							"better greedy run\t" + i + "\t" + ilsDistribution.getCurrentCosts() + "\t"
									+ (System.currentTimeMillis() - decideStartTime)
									+ "ms");
				}
				logger.debug("Improved pertubation\t" + ilsDistribution.getCurrentCosts());
			}
			else {
				if (saveIlsStats) {
					printIlsLog(
							"discard greedy run\t" + i + "\t" + ilsDistribution.getCurrentCosts() + "\t"
									+ (System.currentTimeMillis() - decideStartTime)
									+ "ms");
				}
			}
			if (saveIlsStats) logIlsStep(ilsDistribution);
		}



		// TODO Test: Unify local queries
		//		Map<Integer, Double> queryLocalities = bestDistribution.getQueryLoclities().entrySet().stream()
		//				.sorted(Map.Entry.<Integer, Double>comparingByValue())
		//				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
		//						(e1, e2) -> e1, LinkedHashMap::new));
		//		for (Entry<Integer, Double> query : queryLocalities.entrySet()) {
		//			if (query.getValue() > 0.5)
		//				bestDistribution = unifyQueryAtLargestPartition(query.getKey(), workerIds, bestDistribution);
		//		}


		int movedVertices = bestDistribution.calculateMovedVertices();
		//		logger.info("+++++++++++++ Stopped deciding after " + i + " ILS iterations in " + (System.currentTimeMillis() - decideStartTime)
		//				+ "ms moving "
		//				+ movedVertices);


		String workerVertsMsg = "workerVertives: ";
		for (Integer worker : bestDistribution.getQueryMachines().keySet()) {
			workerVertsMsg += worker + ": " + bestDistribution.getQueryMachines().get(worker).totalVertices + ", ";
		}
		printIlsLog(workerVertsMsg);
		String workerBalanceMsg = "workerBalances: ";
		for (Integer worker : bestDistribution.getQueryMachines().keySet()) {
			workerBalanceMsg += worker + ": " + bestDistribution.getWorkerTotalVerticesImbalanceFactor(worker) + ", ";
		}
		printIlsLog(workerBalanceMsg);
		printIlsLog("worker query chunks");
		bestDistribution.printMoveDistribution(ilsLogWriter);

		logIlsStep(bestDistribution);
		VertexMoveDecision moveDecission;
		if (movedVertices < MinMoveTotalVertices) {
			String printMsg = "Decided not move, not enough vertices: " + totalVerticesMoved + " costs: "
					+ bestDistribution.getCurrentCosts()
					+ " imbalance: " + bestDistribution.getAverageTotalVerticesImbalanceFactor();
			printIlsLog(printMsg);
			logger.info(printMsg);
			moveDecission = null;
		}
		else {
			if (checkTotalVertsOkOrBetter(originalDistribution, originalDistribution)) {
				moveDecission = bestDistribution.toMoveDecision(workerIds, QueryKeepLocalThreshold);
				String printMsg = "Decided move, moves: " + moveDecission.moveMessages + " movedVertices: " + movedVertices + " costs: "
						+ bestDistribution.getCurrentCosts()
						+ " imbalance: "
						+ bestDistribution.getAverageTotalVerticesImbalanceFactor();
				printIlsLog(printMsg);
				logger.info(printMsg);
				moveDecission.printDecission(ilsLogWriter);
			}
			else {
				String printMsg = "ERROR, no move, result with too high imbalance movedVertices: " + totalVerticesMoved + " costs: "
						+ bestDistribution.getCurrentCosts()
						+ " imbalance: " + bestDistribution.getAverageTotalVerticesImbalanceFactor();
				printIlsLog(printMsg);
				logger.error(printMsg);
				moveDecission = null;
			}
		}

		if (saveIlsStats) {
			try (PrintWriter writer = new PrintWriter(new FileWriter(ilsLogDir + File.separator + ilsRunNumber + "_steps.csv"))) {
				writer.println("Time;Costs;CurrentlyBest;LastPertubationCosts;TotalImbalance");
				for (IlsLogItem ilsLogItem : ilsStepsLog) {
					writer.println((ilsLogItem.time + ";"
							+ ilsLogItem.costs + ";" + ilsLogItem.currentlyBestDistributionCosts + ";" + ilsLogItem.lastPertubationCosts
							+ ";" + ilsLogItem.vertTotalImbalance));
				}
				ilsLogWriter.close();
			}
			catch (Exception e) {
				logger.error("Save ILS failed", e);
			}
		}
		ilsRunNumber++;

		return moveDecission;
	}


	private void logIlsStep(QueryDistribution currentDistribution) {
		ilsStepsLog.add(
				new IlsLogItem(System.currentTimeMillis() - decideStartTime, currentDistribution.getCurrentCosts(),
						latestPertubatedDistributionCosts, currentlyBestDistributionCosts,
						currentDistribution.getAverageTotalVerticesImbalanceFactor()));
	}

	private void printIlsLog(String message) {
		ilsLogWriter.println(System.currentTimeMillis() - decideStartTime + "\t" + message);
	}


	private QueryDistribution optimizeGreedy(Set<Integer> queryIds, List<Integer> workerIds, QueryDistribution baseDistribution,
			List<Integer> clusterIds) {
		//		long minActiveVertices = (long) (baseDistribution.workerActiveVertices / workerIds.size() * (1.0 - VertexWorkerActiveImbalance));
		//		long maxActiveVertices = Math.min(
		//				(long) (baseDistribution.workerActiveVertices / workerIds.size() / (1.0 - VertexWorkerActiveImbalance)),
		//				Long.MAX_VALUE / 4);
		long minTotalVertices = (long) (baseDistribution.workerTotalVertices / workerIds.size() * (1.0 - VertexWorkerTotalImbalance));
		long maxTotalVertices = Math.min(
				(long) (baseDistribution.workerTotalVertices / workerIds.size() / (1.0 - VertexWorkerTotalImbalance)),
				Long.MAX_VALUE / 4);

		QueryDistribution bestDistribution = baseDistribution.clone();


		//		// Get query localities
		//		Map<Integer, Double> queryLocalities = bestDistribution.getQueryLoclities();
		//		Map<Integer, Integer> queryLargestPartitions = bestDistribution.getQueryLargestPartitions();
		//		if (saveIlsStats) {
		//			printIlsLog("queryVertices\t" + bestDistribution.queryVertices);
		//			printIlsLog("queryLocalities\t" + queryLocalities);
		//			printIlsLog("queryLargestPartitions\t" + queryLargestPartitions);
		//		}
		//
		//		// Find non-keep-local-queries that can be moved
		//		Map<Integer, Integer> localQueries = new HashMap<>(); // Queries with high locality (key) and their largest partition (value)
		//		IntSet nonlocalQueries = new IntOpenHashSet();
		//		for (Entry<Integer, Double> query : queryLocalities.entrySet()) {
		//			if (query.getValue() > QueryKeepLocalThreshold) {
		//				localQueries.put(query.getKey(), queryLargestPartitions.get(query.getKey()));
		//			}
		//			else {
		//				nonlocalQueries.add(query.getKey());
		//			}
		//		}
		//		if (saveIlsStats) {
		//			printIlsLog("nonlocalQueries\t" + nonlocalQueries);
		//			printIlsLog("localQueries\t" + localQueries);
		//			String msg = ("workerBalances\t");
		//			for (int worker : workerIds) {
		//				msg += (worker + ": "
		//						+ (double) ((int) (baseDistribution.getWorkerActiveVerticesImbalanceFactor(worker) * 100)) / 100 + "/"
		//						+ (double) ((int) (baseDistribution.getWorkerTotalVerticesImbalanceFactor(worker) * 100)) / 100 + " ");
		//			}
		//			printIlsLog(msg);
		//		}

		//		// Find non-keep-local-queries that can be moved
		//				Map<Integer, Integer> localQueries = new HashMap<>(); // Queries with high locality (key) and their largest partition (value)
		//				IntSet nonlocalQueries = new IntOpenHashSet();
		//				for (Entry<Integer, Double> query : queryLocalities.entrySet()) {
		//					if (query.getValue() > QueryKeepLocalThreshold) {
		//						localQueries.put(query.getKey(), queryLargestPartitions.get(query.getKey()));
		//					}
		//					else {
		//						nonlocalQueries.add(query.getKey());
		//					}
		//				}
		//				if (saveIlsStats) {
		//					printIlsLog("nonlocalQueries\t" + nonlocalQueries);
		//					printIlsLog("localQueries\t" + localQueries);
		//					String msg = ("workerBalances\t");
		//					for (int worker : workerIds) {
		//						msg += (worker + ": "
		//								+ (double) ((int) (baseDistribution.getWorkerActiveVerticesImbalanceFactor(worker) * 100)) / 100 + "/"
		//								+ (double) ((int) (baseDistribution.getWorkerTotalVerticesImbalanceFactor(worker) * 100)) / 100 + " ");
		//					}
		//					printIlsLog(msg);
		//				}



		// Get cluster localities
		Map<Integer, Map<Integer, Integer>> clusterPartitions = new HashMap<>();
		for (Entry<Integer, QueryWorkerMachine> machine : baseDistribution.getQueryMachines().entrySet()) {
			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
				for (int chunkCluster : chunk.clusters) {
					Map<Integer, Integer> clusterVertCounts = clusterPartitions.get(chunkCluster);
					if (clusterVertCounts == null) {
						clusterVertCounts = new HashMap<>();
						clusterPartitions.put(chunkCluster, clusterVertCounts);
					}
					MiscUtil.mapAdd(clusterVertCounts, machine.getKey(), chunk.numVertices);
				}
			}
		}

		Map<Integer, Integer> clusterVertices = new HashMap<>();
		Map<Integer, Double> clusterLocalities = new HashMap<>();
		Map<Integer, Integer> clusterLargestPartitions = new HashMap<>();
		for (Entry<Integer, Map<Integer, Integer>> clusterP : clusterPartitions.entrySet()) {
			int clusterId = clusterP.getKey();
			int largestSize = 0;
			int largest = 0;
			int totalSize = 0;
			for (Entry<Integer, Integer> p : clusterP.getValue().entrySet()) {
				totalSize += p.getValue();
				if (p.getValue() > largestSize) {
					largestSize = p.getValue();
					largest = p.getKey();
				}
			}
			clusterVertices.put(clusterId, totalSize);
			clusterLargestPartitions.put(clusterId, largest);
			clusterLocalities.put(clusterId, (double) largestSize / totalSize);
		}


		if (saveIlsStats) {
			printIlsLog("clusterVertices\t" + clusterVertices);
			printIlsLog("clusterLargestPartitions\t" + clusterLargestPartitions);
			printIlsLog("clusterLocalities\t" + clusterLocalities);
		}



		// Find non-keep-local-queries that can be moved
		Map<Integer, Integer> localClusters = new HashMap<>(); // Cluster with high locality (key) and their largest partition (value)
		IntSet nonlocalClusters = new IntOpenHashSet();
		for (Entry<Integer, Double> cluster : clusterLocalities.entrySet()) {
			if (cluster.getValue() > QueryKeepLocalThreshold) {
				localClusters.put(cluster.getKey(), clusterLargestPartitions.get(cluster.getKey()));
			}
			else {
				nonlocalClusters.add(cluster.getKey());
			}
		}
		if (saveIlsStats) {
			printIlsLog("nonlocalQueries\t" + nonlocalClusters);
			printIlsLog("localQueries\t" + localClusters);
			String msg = ("workerBalances\t");
			for (int worker : workerIds) {
				msg += (worker + ": "
						+ (double) ((int) (baseDistribution.getWorkerTotalVerticesImbalanceFactor(worker) * 100)) / 100 + " ");
			}
			printIlsLog(msg);
		}



		// Greedy iterative move queries
		int i = 0;
		long greedyStartTime = System.currentTimeMillis();
		for (; i < MaxGreedyIterations && (System.currentTimeMillis() - greedyStartTime) < MaxGreedyImproveTime
				&& (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime; i++) {
			QueryDistribution iterBestDistribution = bestDistribution.clone();
			boolean anyImproves = false;
			int bestFrom = 0;
			int bestTo = 0;
			int bestCluster = 0;
			int bestNumMoved = Integer.MAX_VALUE;
			//double iterInitialCosts = bestDistribution.getCurrentCosts();

			for (Integer clusterId : clusterIds) {
				//Integer queryLocality = localQueries.get(queryId);
				Map<Integer, Integer> clusterPartitionVerts = clusterPartitions.get(clusterId);
				if (clusterPartitionVerts == null) continue;

				for (Integer fromWorkerId : workerIds) {
					// Dont move from worker if worker has too few vertices afterwards
					QueryWorkerMachine fromWorker = baseDistribution.getQueryMachines().get(fromWorkerId);
					//					long fromWorkerQueryVertices = MiscUtil.defaultLong(fromWorker.queryVertices.get(queryId));
					//					if (fromWorkerQueryVertices == 0) continue;
					//					if (fromWorker.activeVertices - fromWorkerQueryVertices < minActiveVertices
					//							|| fromWorker.totalVertices - fromWorkerQueryVertices < minTotalVertices)
					//						continue;
					int fromWorkerClusterVertices = MiscUtil.defaultInt(clusterPartitionVerts.get(fromWorkerId));
					if (fromWorkerClusterVertices == 0) continue;
					if (fromWorker.totalVertices - fromWorkerClusterVertices < minTotalVertices)
						continue;

					for (Integer toWorkerId : workerIds) {
						if (fromWorkerId == toWorkerId) continue;

						// Dont move to worker if has too many vertices afterwards
						QueryWorkerMachine toWorker = baseDistribution.getQueryMachines().get(toWorkerId);
						//						long toWorkerQueryVertices = MiscUtil.defaultLong(toWorker.queryVertices.get(queryId));
						//						if (toWorkerQueryVertices == 0) continue;
						//						if (toWorker.activeVertices + toWorkerQueryVertices > maxActiveVertices
						//								|| toWorker.totalVertices + toWorkerQueryVertices > maxTotalVertices)
						//							continue;
						long toWorkerClusterVertices = MiscUtil.defaultInt(clusterPartitionVerts.get(toWorkerId));
						if (toWorkerClusterVertices == 0) continue;
						if (toWorker.totalVertices + toWorkerClusterVertices > maxTotalVertices)
							continue;

						// Only move local queries to their largest partition
						//						if (queryLocality != null && !queryLocality.equals(toWorkerId)) continue;

						// Only move smaller to larger partition
						//						if (fromWorkerQueryVertices > toWorkerQueryVertices) continue;
						if (fromWorkerClusterVertices > toWorkerClusterVertices) continue;

						// TODO Optimize, neglect cases.

						QueryDistribution newDistribution = bestDistribution.clone();
						// TODO Move chunks
						int moved = newDistribution.moveAllClusterVertices(clusterId, fromWorkerId, toWorkerId);
						if (moved == 0) continue;

						boolean isValid = checkTotalVertsOkOrBetter(baseDistribution, newDistribution);
						//						boolean isValid = checkActiveVertsOkOrBetter(baseDistribution, newDistribution)
						//								&& checkTotalVertsOkOrBetter(baseDistribution, newDistribution);
						if (!isValid) continue;

						if ((newDistribution.getCurrentCosts() < iterBestDistribution.getCurrentCosts())
								//								||								(newDistribution.getCurrentCosts() == iterBestDistribution.getCurrentCosts()
								//										&& newDistribution.getCurrentCosts() < iterInitialCosts && moved < bestNumMoved)
								) {
							iterBestDistribution = newDistribution;
							anyImproves = true;
							bestFrom = fromWorkerId;
							bestTo = toWorkerId;
							bestCluster = clusterId;
							bestNumMoved = moved;
						}
					}
				}
			}

			if (!checkTotalVertsOkOrBetter(baseDistribution, iterBestDistribution)) {
				logger.error("iterBestDistribution not balanced: " + iterBestDistribution.getAverageTotalVerticesImbalanceFactor());
				printIlsLog("iterBestDistribution not balanced: " + iterBestDistribution.getAverageTotalVerticesImbalanceFactor());
				break;
			}

			if (saveIlsStats) {
				logIlsStep(iterBestDistribution);
				StringBuilder sb = new StringBuilder();
				for (Entry<Integer, QueryWorkerMachine> machine : iterBestDistribution.getQueryMachines().entrySet()) {
					sb.append(machine.getKey() + ": " + machine.getValue().totalVertices + ", ");
				}
				printIlsLog(sb.toString());
			}

			if (anyImproves) {
				if (saveIlsStats) {
					printIlsLog("Improve " + i + " " + bestCluster + " " + bestFrom + "->" + bestTo + " " + bestNumMoved);
				}
			}
			else {
				if (saveIlsStats) {
					printIlsLog("No more improves after " + i + " " + iterBestDistribution.getCurrentCosts());
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
		//					printIlsLog("No more improves after\t" + i + " " + iterBestDistribution.getCurrentCosts());
		//				}
		//				logger.info("No more improves after " + i + " " + iterBestDistribution.getCurrentCosts());
		//				break;
		//			}
		//			bestDistribution = iterBestDistribution;
		//			//totalVerticesMoved += verticesMovedIter;
		//		}
		//		if (saveIlsStats) {
		//			printIlsLog("Finished greedy after\t" + i + " " + bestDistribution.getCurrentCosts());
		//		}
		//		logger.info("+ Stopped greedy after " + i + " iterations in " + (System.currentTimeMillis() - decideStartTime) + "ms" + " "
		//				+ bestDistribution.getCurrentCosts());
		return bestDistribution;
	}


	/**
	 * Pertubation by moving all partitions of a query to machine with largest partition
	 */
	private QueryDistribution pertubationClusterUnifyLargestPartition(List<Integer> clusterIds, List<Integer> workerIds,
			QueryDistribution baseDistribution, Random rd) {
		// First unify random query
		QueryDistribution newDistribution = unifyClusterAtLargestPartition(getRandomFromList(clusterIds, rd), workerIds, baseDistribution);
		newDistribution = balanceDistribution(newDistribution);

		return newDistribution;
	}

	private QueryDistribution balanceDistribution(QueryDistribution baseDistribution) {
		QueryDistribution newDistribution = baseDistribution.clone();

		//		int origMinLoadedId = newDistribution.getMachineMinTotalVertices().id;
		double avgImbalance = newDistribution.getAverageTotalVerticesImbalanceFactor();
		while (!workloadTotalBalanceOk(newDistribution) && (System.currentTimeMillis() - decideStartTime) < MaxTotalImproveTime) {
			//while (!workloadTotalBalanceOk(newDistribution)) {
			int minLoadedId = newDistribution.getMachineMinTotalVertices().id;
			int maxLoadedId = newDistribution.getMachineMaxTotalVertices().id;
			//			if (maxLoadedId == origMinLoadedId) {
			//				System.err.println("Circle detection " + maxLoadedId + "->" + minLoadedId + " max was min");
			//				printIlsLog("Circle detection " + maxLoadedId + "->" + minLoadedId + " max was min");
			//				break;
			//			}

			//int moveCluster = newDistribution.getQueryMachines().get(maxLoadedId).getSmallestCluster();
			int moveQuery = newDistribution.getQueryMachine(maxLoadedId).getSmallestQuery();
			if (moveQuery == -1) {
				printIlsLog("No valid move found for " + maxLoadedId + "->" + minLoadedId);
				break;
			}

			QueryDistribution newDistributionTmp = newDistribution.clone();
			//int moved = newDistributionTmp.moveAllClusterVertices(moveCluster, maxLoadedId, minLoadedId);
			int moved = newDistributionTmp.moveAllQueryVertices(moveQuery, maxLoadedId, minLoadedId, true);
			double movedImbalance = newDistributionTmp.getAverageTotalVerticesImbalanceFactor();
			printIlsLog("Balance move " + moveQuery + " " + maxLoadedId + "->" + minLoadedId + " " + moved + " now " + movedImbalance);

			// Reverse move if necessary
			while (newDistributionTmp.getQueryMachine(minLoadedId).totalVertices > newDistributionTmp
					.getQueryMachine(maxLoadedId).totalVertices) {
				QueryDistribution reverseDistributionTmp = newDistributionTmp.clone();
				moveQuery = newDistributionTmp.getQueryMachine(minLoadedId).getSmallestQuery();
				moved = reverseDistributionTmp.moveAllQueryVertices(moveQuery, minLoadedId, maxLoadedId, true);

				double imbalanceTmp = reverseDistributionTmp.getAverageTotalVerticesImbalanceFactor();
				printIlsLog("Balance reverse move " + moveQuery + " " + minLoadedId + "->" + maxLoadedId + " " + moved + " now "
						+ imbalanceTmp);

				//				printIlsLog("after reverse: " + newDistributionTmp.getQueryMachine(minLoadedId).totalVertices + " "
				//						+ newDistributionTmp.getQueryMachine(maxLoadedId).totalVertices);

				if (imbalanceTmp > movedImbalance) {
					printIlsLog("Balance reverse made it worse " + moveQuery + " " + minLoadedId + "->" + maxLoadedId + " " + moved
							+ " now " + imbalanceTmp);
					break;
				}
				newDistributionTmp = reverseDistributionTmp;
			}

			//			printIlsLog("after move: " + newDistributionTmp.getQueryMachine(minLoadedId).totalVertices + " "
			//					+ newDistributionTmp.getQueryMachine(maxLoadedId).totalVertices);

			double newAvgImbalance = newDistributionTmp.getAverageTotalVerticesImbalanceFactor();
			if (newAvgImbalance > avgImbalance) {
				printIlsLog("Balance step made it worse " + newAvgImbalance + " " + avgImbalance);
				break;
			}

			avgImbalance = newAvgImbalance;
			newDistribution = newDistributionTmp;
		}
		return newDistribution;
	}


	//	/**
	//	 * Moving all partitions of a query to machine with largest partition
	//	 */
	//	private QueryDistribution unifyQuerytLargestPartition(int pertubationQuery, List<Integer> workerIds, QueryDistribution baseDistribution) {
	//		int bestWorkerId = 0;
	//		long bestWorkerPartitionSize = 0;
	//		for (Entry<Integer, QueryWorkerMachine> machine : baseDistribution.getQueryMachines().entrySet()) {
	//			long partitionSize = MiscUtil.defaultLong(machine.getValue().queryVertices.get(pertubationQuery));
	//			if (partitionSize > bestWorkerPartitionSize) {
	//				bestWorkerPartitionSize = partitionSize;
	//				bestWorkerId = machine.getKey();
	//			}
	//		}
	//
	//		if (saveIlsStats) {
	//			printIlsLog("unifyQuerytLargestPartition " + pertubationQuery + " at " + bestWorkerId + " " + bestWorkerPartitionSize + "/"
	//					+ baseDistribution.queryVertices.get(pertubationQuery));
	//		}
	//
	//		return unifyQueryAtWorker(pertubationQuery, workerIds, bestWorkerId, baseDistribution);
	//	}

	/**
	 * Moving all partitions of a query to machine with largest partition
	 */
	private QueryDistribution unifyClusterAtLargestPartition(int pertubationCluster, List<Integer> workerIds,
			QueryDistribution baseDistribution) {
		int bestWorkerId = 0;
		long bestWorkerPartitionSize = 0;
		for (Entry<Integer, QueryWorkerMachine> machine : baseDistribution.getQueryMachines().entrySet()) {
			int partitionSize = MiscUtil.defaultInt(machine.getValue().getClusterVertices().get(pertubationCluster));
			if (partitionSize > bestWorkerPartitionSize) {
				bestWorkerPartitionSize = partitionSize;
				bestWorkerId = machine.getKey();
			}
		}

		if (saveIlsStats) {
			printIlsLog(
					"unifyClusterAtLargestPartition " + pertubationCluster + " at " + bestWorkerId + " " + bestWorkerPartitionSize + "/"
							+ baseDistribution.queryVertices.get(pertubationCluster));
		}

		return unifyClusterAtWorker(pertubationCluster, workerIds, bestWorkerId, baseDistribution);
	}

	//	/**
	//	 * Moving all partitions of a query to machine with largest partition
	//	 */
	//	private QueryDistribution unifyQueryAtWorker(int query, List<Integer> workerIds, int targetWorkerId, QueryDistribution baseDistribution) {
	//		QueryDistribution newDistribution = baseDistribution.clone();
	//		for (int worker : workerIds) {
	//			if (worker != targetWorkerId) {
	//				newDistribution.moveAllQueryVertices(query, worker, targetWorkerId, true);
	//			}
	//		}
	//		return newDistribution;
	//	}

	/**
	 * Moving all partitions of a query to machine with largest partition
	 */
	private QueryDistribution unifyClusterAtWorker(int clusterQuery, List<Integer> workerIds, int targetWorkerId,
			QueryDistribution baseDistribution) {
		QueryDistribution newDistribution = baseDistribution.clone();
		for (int worker : workerIds) {
			if (worker != targetWorkerId) {
				newDistribution.moveAllClusterVertices(clusterQuery, worker, targetWorkerId);
			}
		}
		return newDistribution;
	}



	//	/**
	//	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	//	 */
	//	private boolean workloadActiveBalanceOk(QueryDistribution distribution) {
	//		if (VertexAvgActiveImbalance >= 1 && VertexWorkerActiveImbalance >= 1) return true;
	//		Map<Integer, QueryWorkerMachine> workers = distribution.getQueryMachines();
	//		double avgImbalance = 0.0;
	//		for (Integer worker : workers.keySet()) {
	//			double imb = distribution.getWorkerActiveVerticesImbalanceFactor(worker);
	//			if (imb > VertexWorkerActiveImbalance) return false;
	//			avgImbalance += imb;
	//		}
	//		return (avgImbalance / workers.size()) <= VertexAvgActiveImbalance;
	//	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean workloadTotalBalanceOk(QueryDistribution distribution) {
		if (VertexAvgTotalImbalance >= 1 && VertexWorkerTotalImbalance >= 1) return true;
		Map<Integer, QueryWorkerMachine> workers = distribution.getQueryMachines();
		double avgImbalance = 0.0;
		for (Integer worker : workers.keySet()) {
			double imb = distribution.getWorkerTotalVerticesImbalanceFactor(worker);
			if (imb > VertexWorkerTotalImbalance) return false;
			avgImbalance += imb;
		}
		return (avgImbalance / workers.size()) <= VertexAvgTotalImbalance;
	}


	//	/**
	//	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	//	 */
	//	private boolean checkActiveVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution) {
	//		if (VertexAvgActiveImbalance >= 1 && VertexWorkerActiveImbalance >= 1) return true;
	//		Map<Integer, QueryWorkerMachine> workers = oldDistribution.getQueryMachines();
	//		double oldAvgImbalance = 0.0;
	//		double newAvgImbalance = 0.0;
	//		for (Integer worker : workers.keySet()) {
	//			double oldImb = oldDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
	//			double newImb = newDistribution.getWorkerActiveVerticesImbalanceFactor(worker);
	//			if (newImb > VertexWorkerActiveImbalance && newImb > oldImb) return false;
	//			oldAvgImbalance += oldImb;
	//			newAvgImbalance += newImb;
	//		}
	//		return (newAvgImbalance / workers.size()) <= VertexAvgActiveImbalance || newAvgImbalance <= oldAvgImbalance;
	//	}

	/**
	 * Checks if a new distribution is better than the old one and has sufficient workload balancing at all workers.
	 */
	private boolean checkTotalVertsOkOrBetter(QueryDistribution oldDistribution, QueryDistribution newDistribution) {
		if (VertexAvgTotalImbalance >= 1 && VertexWorkerTotalImbalance >= 1) return true;
		Map<Integer, QueryWorkerMachine> workers = oldDistribution.getQueryMachines();
		double oldAvgImbalance = 0.0;
		double newAvgImbalance = 0.0;
		for (Integer worker : workers.keySet()) {
			double oldImb = oldDistribution.getWorkerTotalVerticesImbalanceFactor(worker);
			double newImb = newDistribution.getWorkerTotalVerticesImbalanceFactor(worker);
			if (newImb > VertexWorkerTotalImbalance && newImb > oldImb) return false;
			oldAvgImbalance += oldImb;
			newAvgImbalance += newImb;
		}
		return (newAvgImbalance / workers.size()) <= VertexAvgTotalImbalance || newAvgImbalance <= oldAvgImbalance;
	}


	// Testing
	public static void main(String[] args) throws Exception {
		Map<String, String> overrideConfigs = new HashMap<>();
		overrideConfigs.put("MASTER_QUERY_MOVE_CALC_TIMEOUT", "999999999");
		overrideConfigs.put("SaveIlsStats", "true");
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
		if (decission == null) {
			System.out.println("No decission");
			return;
		}
		decission.printDecission(System.out);
	}

	private static <T> T getRandomFromList(List<T> list, Random rd) {
		return list.get(rd.nextInt(list.size()));
	}


	private class IlsLogItem {

		public final long time;
		public final double costs;
		public final double lastPertubationCosts;
		public final double currentlyBestDistributionCosts;
		public final double vertTotalImbalance;

		public IlsLogItem(long time, double costs, double lastPertubationCosts, double currentlyBestDistributionCosts,
				double vertTotalImbalance) {
			super();
			this.time = time;
			this.costs = costs;
			this.lastPertubationCosts = lastPertubationCosts;
			this.currentlyBestDistributionCosts = currentlyBestDistributionCosts;
			this.vertTotalImbalance = vertTotalImbalance;
		}
	}
}
