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
	private final List<Integer> workerIds;

	// Move operations and costs so far
	private final Set<VertexMoveOperation> moveOperationsSoFar;
	private double moveCostsSoFar;
	private final double distributionCosts;


	/**
	 * Constructor for initial state, no moves so far.
	 */
	public QueryDistribution(List<Integer> workerIds, Map<Integer, QueryWorkerMachine> queryMachines) {
		this(workerIds, queryMachines, new HashSet<>(), 0);
	}

	/**
	 * Copy constructor
	 */
	public QueryDistribution(List<Integer> workerIds, Map<Integer, QueryWorkerMachine> queryMachines,
			Set<VertexMoveOperation> moveOperationsSoFar, double moveCostsSoFar) {
		super();
		this.workerIds = workerIds;
		this.queryMachines = queryMachines;
		this.moveOperationsSoFar = moveOperationsSoFar;
		this.moveCostsSoFar = moveCostsSoFar;
		this.distributionCosts = calculateVerticesSeparatedCosts();
	}

	@Override
	public QueryDistribution clone() {
		Map<Integer, QueryWorkerMachine> queryMachinesClone = new HashMap<>(queryMachines.size());
		for (Entry<Integer, QueryWorkerMachine> entry : queryMachines.entrySet()) {
			queryMachinesClone.put(entry.getKey(), entry.getValue().createClone());
		}
		return new QueryDistribution(workerIds, queryMachinesClone,
				new HashSet<>(moveOperationsSoFar), moveCostsSoFar);
	}


	/**
	 * TODO Handle intersections
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @param moveIntersects If true, vertices of intersecting queries will be moved as well
	 * @return Number ov moved vertices, 0 if no move possible
	 */
	public int moveVertices(int queryId, int fromWorker, int toWorker, boolean moveIntersects) {
		VertexMoveOperation moveOperation = new VertexMoveOperation(queryId, fromWorker, toWorker);
		if (moveOperationsSoFar.contains(moveOperation))
			return 0;

		// Vertices in this query moved
		Map<Integer, Integer> queryWorkerVertices = actQueryWorkerActiveVerts.get(queryId);
		int toMoveVerts = queryWorkerVertices.get(fromWorker);
		if (moveIntersects) {
			throw new RuntimeException("NIY"); // TODO
		}
		queryWorkerVertices.put(fromWorker, 0);
		queryWorkerVertices.put(toWorker, queryWorkerVertices.get(toWorker) + toMoveVerts);

		if (moveIntersects) {
			// TODO
			//			Map<Integer, Map<Integer, Integer>> fromWorkerAllIntersects = actQueryWorkerIntersects.get(fromWorker);
			//			Map<Integer, Map<Integer, Integer>> toWorkerAllIntersects = actQueryWorkerIntersects.get(fromWorker);
			//
			//			// Als moved intersecting vertices
			//			Map<Integer, Integer> fromWorkerQueryIntersects = fromWorkerAllIntersects.get(queryId);
			//			for (Entry<Integer, Integer> qInters : fromWorkerQueryIntersects.entrySet()) {
			//				toWorkerAllIntersects.put(), value)
			//				queryWorkerVertices.put(fromWorker, 0);
			//				toMoveVerts += qInters.getValue();
			//				fromWorkerQueryIntersects.put(qInters.getKey(), 0);
			//			}
		}

		moveOperationsSoFar.add(moveOperation);
		moveCostsSoFar += (double) toMoveVerts * vertexMoveCosts;
		return toMoveVerts;
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

	private double calculateVerticesSeparatedCosts() {
		double costs = 0;
		for (Map<Integer, Integer> queryWorkerVertices : actQueryWorkerActiveVerts.values()) {
			// Find largest partition
			Integer largestPartition = null;
			int largestPartitionSize = -1;
			for (Entry<Integer, Integer> partition : queryWorkerVertices.entrySet()) {
				if (partition.getValue() > largestPartitionSize) {
					largestPartition = partition.getKey();
					largestPartitionSize = partition.getValue();
				}
			}

			// Calculate vertices separated from largest partition
			for (Entry<Integer, Integer> partition : queryWorkerVertices.entrySet()) {
				if (partition.getKey() != largestPartition) {
					costs += partition.getValue();
				}
			}
		}
		return costs;
	}




	private long getWorkerActiveVertices(int workerId) {
		long verts = 0;
		for (Map<Integer, Integer> queryWorkerVertices : actQueryWorkerActiveVerts.values()) {
			verts += queryWorkerVertices.get(workerId);
		}
		return verts;
	}

	private long getAverageWorkerActiveVertices() {
		long verts = 0;
		for (Entry<Integer, Map<Integer, Integer>> queryWorkerVertices : actQueryWorkerActiveVerts.entrySet()) {
			for (Entry<Integer, Integer> partition : queryWorkerVertices.getValue().entrySet()) {
				verts += partition.getValue();
			}
		}
		return verts / workerIds.size();
	}


	/**
	 * Fraction of vertices away from average vertices.
	 */
	public double getWorkerImbalanceFactor(int workerId) {
		long workerVerts = getWorkerActiveVertices(workerId);
		long avgVerts = getAverageWorkerActiveVertices();
		if (avgVerts == 0) return 0;
		return (double) Math.abs(workerVerts - avgVerts) / avgVerts;
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
		for (Entry<Integer, Map<Integer, Integer>> queryWorkerVertices : actQueryWorkerActiveVerts.entrySet()) {
			System.out.println(queryWorkerVertices.getKey());
			for (Entry<Integer, Integer> partition : queryWorkerVertices.getValue().entrySet()) {
				System.out.println("  " + partition.getKey() + ": " + partition.getValue());
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
