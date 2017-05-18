package mthesis.concurrent_graph.master.vertexmove;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.communication.Messages;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryChunkMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryChunkMessage;
import mthesis.concurrent_graph.util.MiscUtil;

/**
 * Represents a distribution of vertices on worker machines and the sequence to move them in this way
 *
 * @author Jonas Grunert
 *
 */
public class QueryDistribution {

	/** Map<Machine, Map<QueryId, QueryVerticesOnMachine>> */
	private final Map<Integer, QueryWorkerMachine> queryMachines;
	private final Set<Integer> queryIds;

	// Move operations and costs so far
	private double currentCosts;

	public final long workerTotalVertices;
	public final long avgTotalVertices;
	public final long workerActiveVertices;
	public final long avgActiveVertices;
	public final Map<Integer, Long> queryVertices;


	/**
	 * Constructor for initial state, no moves so far.
	 */
	public QueryDistribution(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		this(queryIds, queryMachines, calculateCosts(queryIds, queryMachines), getWorkerTotalVertices(queryMachines),
				getWorkerActiveVertices(queryMachines), getWorkerQueryVertices(queryMachines));
	}

	private static long getWorkerTotalVertices(Map<Integer, QueryWorkerMachine> queryMachines) {
		long vertices = 0;
		for (QueryWorkerMachine worker : queryMachines.values()) {
			vertices += worker.totalVertices;
		}
		return vertices;
	}

	private static long getWorkerActiveVertices(Map<Integer, QueryWorkerMachine> queryMachines) {
		long vertices = 0;
		for (QueryWorkerMachine worker : queryMachines.values()) {
			vertices += worker.activeVertices;
		}
		return vertices;
	}

	private static Map<Integer, Long> getWorkerQueryVertices(Map<Integer, QueryWorkerMachine> queryMachines) {
		Map<Integer, Long> vertices = new HashMap<>();
		for (QueryWorkerMachine worker : queryMachines.values()) {
			for (Entry<Integer, Long> workerQuery : worker.queryVertices.entrySet()) {
				vertices.put(workerQuery.getKey(), MiscUtil.defaultLong(vertices.get(workerQuery.getKey())) + workerQuery.getValue());
			}
		}
		return vertices;
	}

	/**
	 * Copy constructor
	 */
	public QueryDistribution(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines,
			double currentCosts, long workerTotalVertices, long workerActiveVertices, Map<Integer, Long> queryVertices) {
		super();
		this.queryIds = queryIds;
		this.queryMachines = queryMachines;
		this.currentCosts = currentCosts;
		this.workerTotalVertices = workerTotalVertices;
		this.avgTotalVertices = workerTotalVertices / queryMachines.size();
		this.workerActiveVertices = workerActiveVertices;
		this.avgActiveVertices = workerActiveVertices / queryMachines.size();
		this.queryVertices = queryVertices;
	}

	@Override
	public QueryDistribution clone() {
		Map<Integer, QueryWorkerMachine> queryMachinesClone = new HashMap<>(queryMachines.size());
		for (Entry<Integer, QueryWorkerMachine> entry : queryMachines.entrySet()) {
			queryMachinesClone.put(entry.getKey(), entry.getValue().createClone());
		}
		return new QueryDistribution(queryIds, queryMachinesClone, currentCosts, workerTotalVertices, workerActiveVertices,
				new HashMap<>(queryVertices));
	}


	/**
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @param moveIntersects If true, vertices of intersecting queries will be moved as well
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveAllQueryVertices(int queryId, int fromWorkerId, int toWorkerId, boolean moveIntersecting) {
		if (fromWorkerId == toWorkerId) return 0;

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		List<QueryVertexChunk> moved = fromWorker.removeAllQueryVertices(queryId, moveIntersecting);
		for (QueryVertexChunk chunk : moved) {
			toWorker.addQueryChunk(chunk);
		}

		int movedCount = 0;
		for (QueryVertexChunk movedQ : moved) {
			movedCount += movedQ.numVertices;
		}

		currentCosts = calculateCosts(queryIds, queryMachines);
		return movedCount;
	}


	/**
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @param localQueries Local queries, can move only to their largest partition.
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveAllQueryVertices(int queryId, int fromWorkerId, int toWorkerId, Map<Integer, Integer> localQueries) {
		if (fromWorkerId == toWorkerId) return 0;

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		List<QueryVertexChunk> moved = fromWorker.removeAllQueryVertices(fromWorkerId, queryId, toWorkerId, localQueries);
		for (QueryVertexChunk chunk : moved) {
			toWorker.addQueryChunk(chunk);
		}

		int movedCount = 0;
		for (QueryVertexChunk movedQ : moved) {
			movedCount += movedQ.numVertices;
		}

		currentCosts = calculateCosts(queryIds, queryMachines);
		return movedCount;
	}


	/**
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @param localQueries Local queries, can move only to their largest partition.
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveAllClusterVertices(int clusterId, int fromWorkerId, int toWorkerId) {
		if (fromWorkerId == toWorkerId) return 0;

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		List<QueryVertexChunk> moved = fromWorker.removeAllClusterVertices(fromWorkerId, clusterId, toWorkerId);
		for (QueryVertexChunk chunk : moved) {
			toWorker.addQueryChunk(chunk);
		}

		int movedCount = 0;
		for (QueryVertexChunk movedQ : moved) {
			movedCount += movedQ.numVertices;
		}

		currentCosts = calculateCosts(queryIds, queryMachines);
		return movedCount;
	}

	/**
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveAllChunkVertices(IntSet chunkQueries, int fromWorkerId, int toWorkerId) {
		if (fromWorkerId == toWorkerId) return 0;

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		List<QueryVertexChunk> moved = fromWorker.removeAllQueryChunkVertices(chunkQueries);
		for (QueryVertexChunk chunk : moved) {
			toWorker.addQueryChunk(chunk);
		}

		int movedCount = 0;
		for (QueryVertexChunk movedQ : moved) {
			movedCount += movedQ.numVertices;
		}

		currentCosts = calculateCosts(queryIds, queryMachines);
		return movedCount;
	}

	/**
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveSingleChunkVertices(QueryVertexChunk chunk, int fromWorkerId, int toWorkerId) {
		if (fromWorkerId == toWorkerId) return 0;

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		int movedCount = 0;
		if (fromWorker.removeSingleQueryChunkVertices(chunk)) {
			toWorker.addQueryChunk(chunk);
			movedCount = chunk.numVertices;
		}

		currentCosts = calculateCosts(queryIds, queryMachines);
		return movedCount;
	}


	/**
	 * Returns costs of the current distribution
	 */
	public double getCurrentCosts() {
		return currentCosts;
	}

	private static double calculateCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		//return calculateQueryPartitionsCosts(queryIds, queryMachines);
		return calculateVerticesSeparatedCosts(queryIds, queryMachines);
		//return calculateCutCosts(queryIds, queryMachines);
	}

	private static double calculateQueryPartitionsCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		double costs = 0;
		for (Integer queryId : queryIds) {
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				Long q = machine.getValue().queryVertices.get(queryId);
				if (q != null && q > 0) {
					costs++;
				}
			}
			costs--;
		}
		return costs;
	}

	private static double calculateVerticesSeparatedCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		double costs = 0;
		for (Integer queryId : queryIds) {
			// Find largest partition
			Integer largestPartitionMachine = null;
			long largestPartitionSize = -1;
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				Long q = machine.getValue().queryVertices.get(queryId);
				if (q != null && q > largestPartitionSize) {
					largestPartitionMachine = machine.getKey();
					largestPartitionSize = q;
				}
			}

			// Calculate vertices separated from largest partition
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				if (machine.getKey() != largestPartitionMachine) {
					Long q = machine.getValue().queryVertices.get(queryId);
					if (q != null) {
						costs += q;
					}
				}
			}
		}
		return costs;
	}

	private static double calculateCutCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		double costs = 0;
		for (Integer queryId : queryIds) { // TODO More efficient
			for (Entry<Integer, QueryWorkerMachine> machine1 : queryMachines.entrySet()) {
				if (MiscUtil.defaultLong(machine1.getValue().queryVertices.get(queryId)) <= 0) continue;
				for (Entry<Integer, QueryWorkerMachine> machine2 : queryMachines.entrySet()) {
					if (machine1.getKey().equals(machine2.getKey())) continue;
					costs += MiscUtil.defaultLong(machine2.getValue().queryVertices.get(queryId));
				}
			}
		}
		return costs;
	}

	//	private static double calculateMoveCostsMachineMax(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
	//		int hightestMachineCost = 0;
	//		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
	//			int machineCosts = 0;
	//			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
	//				if (chunk.homeMachine != machine.getKey())
	//					machineCosts += chunk.numVertices;
	//			}
	//			hightestMachineCost = Math.max(machineCosts, hightestMachineCost);
	//		}
	//		return (double) hightestMachineCost * VertexMoveCosts;
	//	}
	//
	//	private static double calculateMoveCostsTotal(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
	//		int costSum = 0;
	//		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
	//			int machineCosts = 0;
	//			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
	//				if (chunk.homeMachine != machine.getKey()) machineCosts += chunk.numVertices;
	//			}
	//			costSum += machineCosts;
	//		}
	//		return (double) costSum * VertexMoveCosts;
	//	}

	public int calculateMovedVertices() {
		int movedVertices = 0;
		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
			int machineCosts = 0;
			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
				if (chunk.homeMachine != machine.getKey()) machineCosts += chunk.numVertices;
			}
			movedVertices += machineCosts;
		}
		return movedVertices;
	}



	/**
	 * Fraction of active vertices away from average vertices.
	 */
	public double getWorkerActiveVerticesImbalanceFactor(int workerId) {
		long workerVerts = queryMachines.get(workerId).activeVertices;
		if (avgActiveVertices == 0) return 0;
		return (double) Math.abs(workerVerts - avgActiveVertices) / avgActiveVertices;
	}

	/**
	 * Fraction of total vertices away from average vertices.
	 */
	public double getWorkerTotalVerticesImbalanceFactor(int workerId) {
		long workerVerts = queryMachines.get(workerId).totalVertices;
		if (avgTotalVertices == 0) return 0;
		return (double) Math.abs(workerVerts - avgTotalVertices) / avgTotalVertices;
	}

	public double getAverageActiveVerticesImbalanceFactor() {
		double avg = 0;
		for (Integer workerId : queryMachines.keySet()) {
			avg += getWorkerActiveVerticesImbalanceFactor(workerId);
		}
		return avg / queryMachines.size();
	}

	public double getAverageTotalVerticesImbalanceFactor() {
		double avg = 0;
		for (Integer workerId : queryMachines.keySet()) {
			avg += getWorkerTotalVerticesImbalanceFactor(workerId);
		}
		return avg / queryMachines.size();
	}



	//	private double getLoadImbalanceCosts() {
	//		// TODO Also queries per machine?
	//		Map<Integer, Integer> workerVertices = new HashMap<>(workerIds.size());
	//		for (int workerId : workerIds) {
	//			workerVertices.put(workerId, 0);
	//		}
	//		for (Entry<Integer, Map<Integer, Integer>> queryWorkerVertices : actQueryWorkerActiveVerts.entrySet()) {
	//			for (Entry<Integer, Integer> partition : queryWorkerVertices.getValue().entrySet()) {
	//				workerVertices.put(partition.getKey(), workerVertices.get(partition.getKey()) + partition.getValue());
	//			}
	//		}
	//
	//		// TODO Other cost model?
	//		int largest = 0;
	//		int smallest = Integer.MAX_VALUE;
	//		for (int workerId : workerIds) {
	//			largest = Math.max(largest, workerVertices.get(workerId));
	//			smallest = Math.min(smallest, workerVertices.get(workerId));
	//		}
	//		double imbalance = (double) (largest - smallest) / (double) largest;
	//
	//		// TODO Quantify costs instead of only one hard limit?
	//		if (imbalance > 0.6)
	//			return Double.POSITIVE_INFINITY;
	//		else
	//			return 0;
	//	}


	public void printMoveDistribution(PrintWriter writer) {
		for (Entry<Integer, QueryWorkerMachine> queryWorkerMachine : queryMachines.entrySet()) {
			writer.println(queryWorkerMachine.getKey() + " " + queryWorkerMachine.getValue());
			for (QueryVertexChunk queryChunk : queryWorkerMachine.getValue().queryChunks) {
				writer.println("  " + queryChunk);
			}
		}
	}


	public VertexMoveDecision toMoveDecision(List<Integer> workerIds, double QueryKeepLocalThreshold) {
		Map<Integer, Double> queryLocalities = getQueryLoclities();
		Map<Integer, Integer> queryLargestPartitions = getQueryLargestPartitions();
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

		Map<Integer, List<SendQueryChunkMessage>> workerVertSendMsgs = new HashMap<>();
		Map<Integer, List<ReceiveQueryChunkMessage>> workerVertRecvMsgs = new HashMap<>();
		for (int workerId : workerIds) {
			workerVertSendMsgs.put(workerId, new ArrayList<>());
			workerVertRecvMsgs.put(workerId, new ArrayList<>());
		}

		// Map SrcMachine->(TargetMachine->(Query->NumVertices)
		Map<Integer, Map<Integer, Map<Integer, Integer>>> machineMoveIncludeQueries = new HashMap<>();
		Map<Integer, Map<Integer, Map<Integer, Integer>>> machineMoveTolerateQueries = new HashMap<>();
		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
			machineMoveIncludeQueries.put(machine.getKey(), new HashMap<>());
			machineMoveTolerateQueries.put(machine.getKey(), new HashMap<>());
		}

		// Get all moved queries
		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
				if (chunk.homeMachine != machine.getKey()) {
					Map<Integer, Map<Integer, Integer>> srcMachine = machineMoveIncludeQueries.get(chunk.homeMachine);
					Map<Integer, Map<Integer, Integer>> srcMachineTolreate = machineMoveTolerateQueries.get(chunk.homeMachine);
					Map<Integer, Integer> targetMachineSendQueries = srcMachine.get(machine.getKey());
					if (targetMachineSendQueries == null) {
						targetMachineSendQueries = new HashMap<>();
						srcMachine.put(machine.getKey(), targetMachineSendQueries);
						srcMachineTolreate.put(machine.getKey(), new HashMap<>());
					}

					for (int chunkQuery : chunk.queries) {
						targetMachineSendQueries.put(chunkQuery,
								MiscUtil.defaultInt(targetMachineSendQueries.get(chunkQuery)) + chunk.numVertices);
					}
				}
			}
		}

		for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> moveSrc : machineMoveIncludeQueries.entrySet()) {
			Map<Integer, Map<Integer, Integer>> moveSrcTolerate = machineMoveTolerateQueries.get(moveSrc.getKey());

			for (Entry<Integer, Map<Integer, Integer>> moveDst : moveSrc.getValue().entrySet()) {
				Map<Integer, Integer> moveDstTolerate = moveSrcTolerate.get(moveDst.getKey());
				for (Entry<Integer, Integer> moveQuery : new HashMap<>(moveDst.getValue()).entrySet()) {
					//					int queryMoveVertices = moveQuery.getValue();
					Map<Integer, Long> srcMachineVerts = queryMachines.get(moveSrc.getKey()).queryVertices;
					long queryVerticesOnmachine = MiscUtil.defaultLong(srcMachineVerts.get(moveQuery.getKey()));
					// Only tolerate query if more vertices remain on machine
					if (queryVerticesOnmachine > 0) {
						moveDst.getValue().remove(moveQuery.getKey());
						moveDstTolerate.put(moveQuery.getKey(), moveQuery.getValue());
					}
					else {
						// Only tolerate if other move has vertices of this query
						for (Entry<Integer, Map<Integer, Integer>> otherMoveDst : moveSrc.getValue().entrySet()) {
							if (otherMoveDst.getKey().equals(moveDst.getKey())) continue;
							if (MiscUtil.defaultInt(otherMoveDst.getValue().get(moveQuery.getKey())) > 0) {
								moveDst.getValue().remove(moveQuery.getKey());
								moveDstTolerate.put(moveQuery.getKey(), moveQuery.getValue());
							}
						}
					}
				}
			}
		}

		System.out.println("MOVES--------");
		for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> moveSrc : machineMoveIncludeQueries.entrySet()) {
			System.out.println(moveSrc.getKey());
			for (Entry<Integer, Map<Integer, Integer>> move : moveSrc.getValue().entrySet()) {
				System.out.println("\t" + move.getKey() + " " + move.getValue() + " "
						+ machineMoveTolerateQueries.get(moveSrc.getKey()).get(move.getKey()));
			}
		}

		// Remove queries from move chunks if has remainders on src machine
		//		for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> moveSrc : machineMoveQueries.entrySet()) {
		//			for (Entry<Integer, Map<Integer, Integer>> moveDst : moveSrc.getValue().entrySet()) {
		//				for (int moveQuery : new ArrayList<>(moveDst.getValue().keySet())) {
		//					if (MiscUtil.defaultLong(queryMachines.get(moveSrc.getKey()).queryVertices.get(moveQuery)) > 0) {
		//						moveDst.getValue().remove(moveQuery);
		//					}
		//				}
		//			}
		//		}
		// Move chunks on machine if more vertices there or move to machine with most vertices of query moved there
		//		for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> moveSrc : machineMoveQueries.entrySet()) {
		//			for (Entry<Integer, Map<Integer, Integer>> moveDst : moveSrc.getValue().entrySet()) {
		//				for (Entry<Integer, Integer> moveQuery : new HashMap<>(moveDst.getValue()).entrySet()) {
		//					int queryMoveVertices = moveQuery.getValue();
		//					Map<Integer, Long> machineVerts = queryMachines.get(moveSrc.getKey()).queryVertices;
		//					long queryVerticesOnmachine = MiscUtil.defaultLong(machineVerts.get(moveQuery.getKey()));
		//					// Remove if more vertices remain on machine
		//					if (queryMoveVertices < queryVerticesOnmachine) {
		//						moveDst.getValue().remove(moveQuery.getKey());
		//					}
		//					else {
		//						// Remove if other move has more vertices of this query
		//						for (Entry<Integer, Map<Integer, Integer>> otherMoveDst : moveSrc.getValue().entrySet()) {
		//							if (otherMoveDst.getKey().equals(moveDst.getKey())) continue;
		//							if (MiscUtil.defaultInt(otherMoveDst.getValue().get(moveQuery.getKey())) > queryMoveVertices) {
		//								moveDst.getValue().remove(moveQuery.getKey());
		//							}
		//						}
		//					}
		//				}
		//			}
		//		}
		// Remove queries from move chunks that are local
		//		for (Entry<Integer, Map<Integer, Pair<IntSet, Integer>>> moveSrc : machineMoveQueries.entrySet()) {
		//			for (Entry<Integer, Pair<IntSet, Integer>> moveDst : moveSrc.getValue().entrySet()) {
		//				for (int moveQuery : new ArrayList<>(moveDst.getValue().first)) {
		//					if (localQueries.containsKey(moveQuery) && localQueries.get(moveQuery) == moveSrc.getKey()) {
		//						moveDst.getValue().first.rem(moveQuery);
		//					}
		//				}
		//			}
		//		}
		// Remove queries with more vertices on src than on target machine
		//		for (Entry<Integer, Map<Integer, Pair<IntSet, Integer>>> moveSrc : machineMoveQueries.entrySet()) {
		//			for (Entry<Integer, Pair<IntSet, Integer>> moveDst : moveSrc.getValue().entrySet()) {
		//				for (int moveQuery : new ArrayList<>(moveDst.getValue().first)) {
		//					long srcVerts = MiscUtil.defaultLong(queryMachines.get(moveSrc.getKey()).queryVertices.get(moveQuery));
		//					long dstVerts = MiscUtil.defaultLong(queryMachines.get(moveDst.getKey()).queryVertices.get(moveQuery));
		//					if (srcVerts > dstVerts) {
		//						moveDst.getValue().first.rem(moveQuery);
		//					}
		//				}
		//			}
		//		}

		for (Entry<Integer, Map<Integer, Map<Integer, Integer>>> moveSrcInclude : machineMoveIncludeQueries.entrySet()) {
			Map<Integer, Map<Integer, Integer>> moveSrcTolerate = machineMoveTolerateQueries.get(moveSrcInclude.getKey());
			if (moveSrcInclude.getValue().isEmpty()) {
				System.err.println("No include queries, only tolerate " + moveSrcTolerate);
				continue;
			}

			for (Entry<Integer, Map<Integer, Integer>> moveDstInclude : moveSrcInclude.getValue().entrySet()) {
				Map<Integer, Integer> moveDstTolreate = moveSrcTolerate.get(moveDstInclude.getKey());
				if (moveDstTolreate == null) {
					moveDstTolreate = new HashMap<>();
				}

				if (moveDstInclude.getValue().isEmpty()) continue;
				workerVertSendMsgs.get(moveSrcInclude.getKey()).add(
						Messages.ControlMessage.StartBarrierMessage.SendQueryChunkMessage.newBuilder()
								//.setMaxMoveCount(moveDst.getValue().second)
								.setMaxMoveCount(Integer.MAX_VALUE)
								.addAllIncludeQueries(moveDstInclude.getValue().keySet())
								.addAllTolreateQueries(moveDstTolreate.keySet())
								.setMoveToMachine(moveDstInclude.getKey())
								.build());
				workerVertRecvMsgs.get(moveDstInclude.getKey()).add(
						Messages.ControlMessage.StartBarrierMessage.ReceiveQueryChunkMessage.newBuilder()
								.addAllChunkQueries(moveDstInclude.getValue().keySet())
								.setReceiveFromMachine(moveSrcInclude.getKey()).build());
			}
		}

		return new VertexMoveDecision(workerVertSendMsgs, workerVertRecvMsgs);
	}


	public Map<Integer, QueryWorkerMachine> getQueryMachines() {
		return queryMachines;
	}


	public int getMachineMinActiveVertices() {
		int workerId = 0;
		long minVertices = Long.MAX_VALUE;
		for (Entry<Integer, QueryWorkerMachine> worker : queryMachines.entrySet()) {
			if (worker.getValue().activeVertices < minVertices) {
				minVertices = worker.getValue().activeVertices;
				workerId = worker.getKey();
			}
		}
		return workerId;
	}

	public int getMachineMaxActiveVertices() {
		int workerId = 0;
		long maxVertices = 0;
		for (Entry<Integer, QueryWorkerMachine> worker : queryMachines.entrySet()) {
			if (worker.getValue().activeVertices > maxVertices) {
				maxVertices = worker.getValue().activeVertices;
				workerId = worker.getKey();
			}
		}
		return workerId;
	}

	public int getMachineMinTotalVertices() {
		int workerId = 0;
		long minVertices = Long.MAX_VALUE;
		for (Entry<Integer, QueryWorkerMachine> worker : queryMachines.entrySet()) {
			if (worker.getValue().totalVertices < minVertices) {
				minVertices = worker.getValue().totalVertices;
				workerId = worker.getKey();
			}
		}
		return workerId;
	}

	public int getMachineMaxTotalVertices() {
		int workerId = 0;
		long maxVertices = 0;
		for (Entry<Integer, QueryWorkerMachine> worker : queryMachines.entrySet()) {
			if (worker.getValue().totalVertices > maxVertices) {
				maxVertices = worker.getValue().totalVertices;
				workerId = worker.getKey();
			}
		}
		return workerId;
	}


	/**
	 * Returns machine with maximum number of vertices for given query
	 */
	public int getMachineMaxQueryVertices(int queryId) {
		int workerId = 0;
		long maxVertices = 0;
		for (Entry<Integer, QueryWorkerMachine> worker : queryMachines.entrySet()) {
			long queryVertices = MiscUtil.defaultLong(worker.getValue().queryVertices.get(queryId));
			if (queryVertices > maxVertices) {
				maxVertices = queryVertices;
				workerId = worker.getKey();
			}
		}
		return workerId;
	}

	public long getMaxQueryPartitionSize(int queryId) {
		long maxVertices = 0;
		for (Entry<Integer, QueryWorkerMachine> worker : queryMachines.entrySet()) {
			long queryVertices = MiscUtil.defaultLong(worker.getValue().queryVertices.get(queryId));
			if (queryVertices > maxVertices) {
				maxVertices = queryVertices;
			}
		}
		return maxVertices;
	}

	public Map<Integer, Double> getQueryLoclities() {
		Map<Integer, Double> queryLocalities = new HashMap<>();
		for (Integer queryId : queryIds) {
			long maxPartition = getMaxQueryPartitionSize(queryId);
			queryLocalities.put(queryId, (double) maxPartition / queryVertices.get(queryId));
		}
		return queryLocalities;
	}

	public Map<Integer, Integer> getQueryLargestPartitions() {
		Map<Integer, Integer> queryLocalities = new HashMap<>();
		for (Integer queryId : queryIds) {
			queryLocalities.put(queryId, getMachineMaxQueryVertices(queryId));
		}
		return queryLocalities;
	}

	//	// Testing
	//	public static void main(String[] args) {
	//		Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts = new HashMap<>();
	//
	//		Map<Integer, Integer> q0 = new HashMap<>();
	//		q0.put(0, 200);
	//		q0.put(1, 100);
	//		q0.put(2, 100);
	//		actQueryWorkerActiveVerts.put(1, q0);
	//
	//		Map<Integer, Integer> q1 = new HashMap<>();
	//		q1.put(0, 200);
	//		q1.put(1, 100);
	//		q1.put(2, 100);
	//		actQueryWorkerActiveVerts.put(1, q1);
	//
	//		QueryDistribution qd = new QueryDistribution(actQueryWorkerActiveVerts);
	//		System.out.println(qd.getCosts());
	//		System.out.println(qd.moveVertices(1, 1, 0));
	//		System.out.println(qd.getCosts());
	//		System.out.println(qd.moveVertices(1, 1, 0));
	//		System.out.println(qd.getCosts());
	//		System.out.println(qd.moveVertices(1, 2, 0));
	//		System.out.println(qd.getCosts());
	//	}
}
