package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
	private static final double vertexMoveCosts = 0.7; // TODO smarter

	/** Map<QueryId, Map<MachineId, ActiveVertexCount>> */
	//	private final Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts;
	/** Map<Machine, Map<QueryId, QueryVerticesOnMachine>> */
	private final Map<Integer, QueryWorkerMachine> queryMachines;
	private final Set<Integer> queryIds;

	// Move operations and costs so far
	private final Set<VertexMoveOperation> moveOperationsSoFar;
	private double moveCostsSoFar;
	private double distributionCosts;


	/**
	 * Constructor for initial state, no moves so far.
	 */
	public QueryDistribution(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		this(queryIds, queryMachines, new HashSet<>(), 0, calculateVerticesSeparatedCosts(queryIds, queryMachines));
	}

	/**
	 * Copy constructor
	 */
	public QueryDistribution(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines,
			Set<VertexMoveOperation> moveOperationsSoFar, double moveCostsSoFar, double distributionCosts) {
		super();
		this.queryIds = queryIds;
		this.queryMachines = queryMachines;
		this.moveOperationsSoFar = moveOperationsSoFar;
		this.moveCostsSoFar = moveCostsSoFar;
		this.distributionCosts = distributionCosts;
	}

	@Override
	public QueryDistribution clone() {
		Map<Integer, QueryWorkerMachine> queryMachinesClone = new HashMap<>(queryMachines.size());
		for (Entry<Integer, QueryWorkerMachine> entry : queryMachines.entrySet()) {
			queryMachinesClone.put(entry.getKey(), entry.getValue().createClone());
		}
		return new QueryDistribution(queryIds, queryMachinesClone,
				new HashSet<>(moveOperationsSoFar), moveCostsSoFar, distributionCosts);
	}


	/**
	 * TODO Handle intersections
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @param moveIntersects If true, vertices of intersecting queries will be moved as well
	 * @return Number of moved vertices, 0 if no move possible
	 */
	public int moveVertices(int queryId, int fromWorkerId, int toWorkerId, boolean moveIntersecting) {
		VertexMoveOperation moveOperation = new VertexMoveOperation(queryId, fromWorkerId, toWorkerId);
		if (moveOperationsSoFar.contains(moveOperation))
			return 0; // Cant do same move twice?

		QueryWorkerMachine fromWorker = queryMachines.get(fromWorkerId);
		QueryWorkerMachine toWorker = queryMachines.get(toWorkerId);

		Map<Integer, QueryVerticesOnMachine> moved = fromWorker.removeQueryVertices(queryId, moveIntersecting);
		toWorker.addQueryVertices(moved);

		int movedCount  = 0;
		for (QueryVerticesOnMachine movedQ : moved.values()) {
			movedCount += movedQ.totalVertices;
		}

		moveOperationsSoFar.add(moveOperation);
		moveCostsSoFar += (double) movedCount * vertexMoveCosts;
		distributionCosts = calculateVerticesSeparatedCosts(queryIds, queryMachines);
		return movedCount;
	}


	/**
	 * Calculates the costs of this state, sum of mis-distribution and moveCosts so far.
	 *
	 * Mis-distribution costs are a sum of:
	 * 	- Vertices separated from the largest partition
	 *  - TODO Queries not entirely local?
	 *  - TODO Load imbalance?
	 */
	public double getCosts() {
		return moveCostsSoFar + distributionCosts;
	}

	private static double calculateVerticesSeparatedCosts(Set<Integer> queryIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		double costs = 0;
		for (Integer queryId : queryIds) {
			// Find largest partition
			Integer largestPartitionMachine = null;
			int largestPartitionSize = -1;
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				QueryVerticesOnMachine q = machine.getValue().queries.get(queryId);
				if (q != null && q.totalVertices > largestPartitionSize) {
					largestPartitionMachine = machine.getKey();
					largestPartitionSize = q.totalVertices;
				}
			}

			// Calculate vertices separated from largest partition
			for (Entry<Integer, QueryWorkerMachine> machine : queryMachines.entrySet()) {
				if (machine.getKey() != largestPartitionMachine) {
					QueryVerticesOnMachine q = machine.getValue().queries.get(queryId);
					if (q != null) {
						costs += q.totalVertices;
					}
				}
			}
		}
		return costs;
	}



	/**
	 * Fraction of vertices away from average vertices.
	 */
	public double getWorkerImbalanceFactor(int workerId) {
		long workerVerts = queryMachines.get(workerId).activeVertices;
		long avgVerts = getAverageWorkerActiveVertices();
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
			System.out.println(queryWorkerMachine.getKey());
			for (Entry<Integer, QueryVerticesOnMachine> query : queryWorkerMachine.getValue().queries.entrySet()) {
				System.out.println("  " + query.getKey() + ": " + query.getValue().totalVertices);
			}
		}
	}

	public void printMoveDecissions() {
		for (VertexMoveOperation moveOperation : moveOperationsSoFar) {
			System.out.println(moveOperation.QueryId + ": " + moveOperation.FromMachine + "->" + moveOperation.ToMachine);
		}
	}

	public VertexMoveDecision toMoveDecision(List<Integer> workerIds) {
		if (moveOperationsSoFar.isEmpty())
			return null;

		Map<Integer, List<SendQueryVerticesMessage>> workerVertSendMsgs = new HashMap<>();
		Map<Integer, List<ReceiveQueryVerticesMessage>> workerVertRecvMsgs = new HashMap<>();
		for (int workerId : workerIds) {
			workerVertSendMsgs.put(workerId, new ArrayList<>());
			workerVertRecvMsgs.put(workerId, new ArrayList<>());
		}

		for (VertexMoveOperation moveOperation : moveOperationsSoFar) {
			workerVertSendMsgs.get(moveOperation.FromMachine).add(
					Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage.newBuilder()
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
