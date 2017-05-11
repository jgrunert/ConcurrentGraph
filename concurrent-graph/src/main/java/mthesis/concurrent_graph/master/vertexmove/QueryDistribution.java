package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

		List<QueryVertexChunk> moved = fromWorker.removeAllQueryVertices(queryId, toWorkerId, localQueries);
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
		return calculateQueryPartitionsCosts(queryIds, queryMachines);
		//return calculateVerticesSeparatedCosts(queryIds, queryMachines);
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


	public void printMoveDistribution() {
		for (Entry<Integer, QueryWorkerMachine> queryWorkerMachine : queryMachines.entrySet()) {
			System.out.println(queryWorkerMachine.getKey() + " " + queryWorkerMachine.getValue());
			for (QueryVertexChunk queryChunk : queryWorkerMachine.getValue().queryChunks) {
				System.out.println("  " + queryChunk);
			}
		}
	}

	//	public void printMoveDecissions() {
	//		for (Entry<VertexMoveOperation, Integer> moveOperationEntry : moveOperationsSoFar.entrySet()) {
	//			VertexMoveOperation moveOperation = moveOperationEntry.getKey();
	//			System.out.println(moveOperation.QueryId + ": " + moveOperation.FromMachine + "->" + moveOperation.ToMachine + " "
	//					+ moveOperationEntry.getValue());
	//		}
	//	}

	public VertexMoveDecision toMoveDecision(List<Integer> workerIds) {
		Map<Integer, List<SendQueryChunkMessage>> workerVertSendMsgs = new HashMap<>();
		Map<Integer, List<ReceiveQueryChunkMessage>> workerVertRecvMsgs = new HashMap<>();
		for (int workerId : workerIds) {
			workerVertSendMsgs.put(workerId, new ArrayList<>());
			workerVertRecvMsgs.put(workerId, new ArrayList<>());
		}

		// Find all chunks that are not on their home machines
		Set<VertexMoveOperation> allMoves = new HashSet<>();
		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
				if (chunk.homeMachine != machine.getKey()) {
					allMoves.add(new VertexMoveOperation(chunk.queries, chunk.homeMachine, machine.getKey()));
				}
			}
		}

		for (VertexMoveOperation moveOperation : allMoves) {
			workerVertSendMsgs.get(moveOperation.FromMachine).add(
					Messages.ControlMessage.StartBarrierMessage.SendQueryChunkMessage.newBuilder()
							.setMaxMoveCount(Integer.MAX_VALUE)
							.addAllChunkQueries(moveOperation.QueryChunk)
							.setMoveToMachine(moveOperation.ToMachine).setMaxMoveCount(Integer.MAX_VALUE)
							.build());
			workerVertRecvMsgs.get(moveOperation.ToMachine).add(
					Messages.ControlMessage.StartBarrierMessage.ReceiveQueryChunkMessage.newBuilder()
							.addAllChunkQueries(moveOperation.QueryChunk)
							.setReceiveFromMachine(moveOperation.FromMachine).build());
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
