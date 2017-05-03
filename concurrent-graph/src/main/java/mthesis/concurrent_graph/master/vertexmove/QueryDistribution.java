package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.communication.Messages;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage;

/**
 * Represents a distribution of vertices on worker machines and the sequence to move them in this way
 *
 * @author Jonas Grunert
 *
 */
public class QueryDistribution {

	// Move costs per vertex to move, relative to a vertex separated from its larger partition
	private static final double VertexMoveCosts = Configuration.getPropertyDoubleDefault("VertexMoveCosts", 0.7); // TODO smarter

	/** Map<QueryId, Map<MachineId, ActiveVertexCount>> */
	//	private final Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts;
	/** Map<Machine, Map<QueryId, QueryVerticesOnMachine>> */
	private final Map<Integer, QueryWorkerMachine> queryMachines;
	private final Set<Integer> queryIds;

	// Move operations and costs so far
	private double currentCosts;


	/**
	 * Constructor for initial state, no moves so far.
	 */
	public QueryDistribution(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		this(queryIds, queryMachines, calculateCosts(queryIds, queryMachines));
	}

	/**
	 * Copy constructor
	 */
	public QueryDistribution(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines,
			double currentCosts) {
		super();
		this.queryIds = queryIds;
		this.queryMachines = queryMachines;
		this.currentCosts = currentCosts;
	}

	@Override
	public QueryDistribution clone() {
		Map<Integer, QueryWorkerMachine> queryMachinesClone = new HashMap<>(queryMachines.size());
		for (Entry<Integer, QueryWorkerMachine> entry : queryMachines.entrySet()) {
			queryMachinesClone.put(entry.getKey(), entry.getValue().createClone());
		}
		return new QueryDistribution(queryIds, queryMachinesClone, currentCosts);
	}


	/**
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @param moveIntersects If true, vertices of intersecting queries will be moved as well
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveVertices(int queryId, int fromWorkerId, int toWorkerId, boolean moveIntersecting) {
		if (fromWorkerId == toWorkerId) return 0;

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		List<QueryVertexChunk> moved = fromWorker.removeQueryVertices(queryId, moveIntersecting);
		toWorker.addQueryVertices(moved);

		int movedCount = 0;
		for (QueryVertexChunk movedQ : moved) {
			movedCount += movedQ.numVertices;
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
		return calculateVerticesSeparatedCosts(queryIds, queryMachines) + calculateMoveCosts(queryIds, queryMachines);

	}

	private static double calculateVerticesSeparatedCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		// TODO Better, use max instead of sum?
		double costs = 0;
		for (Integer queryId : queryIds) {
			// Find largest partition
			Integer largestPartitionMachine = null;
			int largestPartitionSize = -1;
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				Integer q = machine.getValue().queryVertices.get(queryId);
				if (q != null && q > largestPartitionSize) {
					largestPartitionMachine = machine.getKey();
					largestPartitionSize = q;
				}
			}

			// Calculate vertices separated from largest partition
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				if (machine.getKey() != largestPartitionMachine) {
					Integer q = machine.getValue().queryVertices.get(queryId);
					if (q != null) {
						costs += q;
					}
				}
			}
		}
		return costs;
	}

	private static double calculateMoveCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		// TODO
		int hightestMachineCost = 0;
		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
			int machineCosts = 0;
			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
				if (chunk.homeMachine != machine.getKey())
					machineCosts += chunk.numVertices;
			}
			hightestMachineCost = Math.max(machineCosts, hightestMachineCost);
		}
		return (double) hightestMachineCost * VertexMoveCosts;
	}

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
		long avgVerts = getAverageWorkerActiveVertices();
		if (avgVerts == 0) return 0;
		return (double) Math.abs(workerVerts - avgVerts) / avgVerts;
	}

	/**
	 * Fraction of total vertices away from average vertices.
	 */
	public double getWorkerTotalVerticesImbalanceFactor(int workerId) {
		long workerVerts = queryMachines.get(workerId).totalVertices;
		long avgVerts = getAverageWorkerTotalVertices();
		if (avgVerts == 0) return 0;
		return (double) Math.abs(workerVerts - avgVerts) / avgVerts;
	}

	private long getAverageWorkerActiveVertices() {
		long verts = 0;
		for (QueryWorkerMachine machine : queryMachines.values()) {
			verts += machine.activeVertices;
		}
		return verts / queryMachines.size();
	}

	private long getAverageWorkerTotalVertices() {
		long verts = 0;
		for (QueryWorkerMachine machine : queryMachines.values()) {
			verts += machine.totalVertices;
		}
		return verts / queryMachines.size();
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
		Map<Integer, List<SendQueryVerticesMessage>> workerVertSendMsgs = new HashMap<>();
		Map<Integer, List<ReceiveQueryVerticesMessage>> workerVertRecvMsgs = new HashMap<>();
		for (int workerId : workerIds) {
			workerVertSendMsgs.put(workerId, new ArrayList<>());
			workerVertRecvMsgs.put(workerId, new ArrayList<>());
		}

		// Find all chunks that are not on their home machines
		Set<VertexMoveOperation> allMoves = new HashSet<>();
		for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
			for (QueryVertexChunk chunk : machine.getValue().queryChunks) {
				if (chunk.homeMachine != machine.getKey()) {
					for (int chunkQuery : chunk.queries) {
						allMoves.add(new VertexMoveOperation(chunkQuery, chunk.homeMachine, machine.getKey()));
					}
				}
			}
		}

		// TODO Ist his all correct? Check for cycles etc

		for (VertexMoveOperation moveOperation : allMoves) {
			workerVertSendMsgs.get(moveOperation.FromMachine).add(
					Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage.newBuilder()
					.setMaxMoveCount(Integer.MAX_VALUE)
					.setQueryId(moveOperation.QueryId)
					.setMoveToMachine(moveOperation.ToMachine).setMaxMoveCount(Integer.MAX_VALUE)
					.build());
			workerVertRecvMsgs.get(moveOperation.ToMachine).add(
					Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage.newBuilder()
					.setQueryId(moveOperation.QueryId)
					.setReceiveFromMachine(moveOperation.FromMachine).build());
		}

		return new VertexMoveDecision(workerVertSendMsgs, workerVertRecvMsgs);
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
