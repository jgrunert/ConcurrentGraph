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
	private static final double vertexMoveCosts = 0.7;
	// Relative cost of a vertex imbalanced workload
	private static final double vertexImbalanceCosts = 5.0;

	/** Map<QueryId, Map<MachineId, ActiveVertexCount>> */
	private final Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts;
	private final List<Integer> workerIds;

	// Move operations and costs so far
	private final Set<VertexMoveOperation> moveOperationsSoFar;
	private double moveCostsSoFar;


	public QueryDistribution(List<Integer> workerIds, Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts) {
		this(workerIds, actQueryWorkerActiveVerts, new HashSet<>(), 0);
	}

	public QueryDistribution(List<Integer> workerIds, Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts,
			Set<VertexMoveOperation> moveOperationsSoFar, double moveCostsSoFar) {
		super();
		this.workerIds = workerIds;
		this.actQueryWorkerActiveVerts = actQueryWorkerActiveVerts;
		this.moveOperationsSoFar = moveOperationsSoFar;
		this.moveCostsSoFar = moveCostsSoFar;
	}

	@Override
	public QueryDistribution clone() {
		Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVertsClone = new HashMap<>(moveOperationsSoFar.size());
		for (Entry<Integer, Map<Integer, Integer>> entry : actQueryWorkerActiveVerts.entrySet()) {
			actQueryWorkerActiveVertsClone.put(entry.getKey(), new HashMap<>(entry.getValue()));
		}
		return new QueryDistribution(workerIds, actQueryWorkerActiveVertsClone, new HashSet<>(moveOperationsSoFar), moveCostsSoFar);
	}


	/**
	 * TODO Handle intersections
	 * @param queryId
	 * @param fromWorker
	 * @param toWorker
	 * @return True if this move operation is possible
	 */
	public boolean moveVertices(int queryId, int fromWorker, int toWorker) {
		VertexMoveOperation moveOperation = new VertexMoveOperation(queryId, fromWorker, toWorker);
		if (moveOperationsSoFar.contains(moveOperation))
			return false;

		Map<Integer, Integer> queryWorkerVertices = actQueryWorkerActiveVerts.get(queryId);
		int toMoveVerts = queryWorkerVertices.get(fromWorker);
		queryWorkerVertices.put(fromWorker, 0);
		queryWorkerVertices.put(toWorker, queryWorkerVertices.get(toWorker) + toMoveVerts);

		moveOperationsSoFar.add(moveOperation);
		moveCostsSoFar += (double) toMoveVerts * vertexMoveCosts;
		return true;
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
		return moveCostsSoFar + getVerticesSeparatedCosts() + getLoadImbalanceCosts();
	}

	private double getVerticesSeparatedCosts() {
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


	/**
	 * Calculates cost of imbalanced workload of active vertices
	 */
	private double getLoadImbalanceCosts() {
		// TODO Also queries per machine?
		Map<Integer, Integer> workerVertices = new HashMap<>(workerIds.size());
		for (int workerId : workerIds) {
			workerVertices.put(workerId, 0);
		}
		for (Entry<Integer, Map<Integer, Integer>> queryWorkerVertices : actQueryWorkerActiveVerts.entrySet()) {
			for (Entry<Integer, Integer> partition : queryWorkerVertices.getValue().entrySet()) {
				workerVertices.put(partition.getKey(), workerVertices.get(partition.getKey()) + partition.getValue());
			}
		}

		// TODO Other cost model?
		long averageVerts = 0;
		for (int workerId : workerIds) {
			averageVerts += workerVertices.get(workerId);
		}
		averageVerts /= workerIds.size();

		double imbalance = 0;
		for (int workerId : workerIds) {
			imbalance += Math.abs(workerVertices.get(workerId) - averageVerts);
		}

		// Imalance factor, divide by two. Otherwise we would count a imbalanced vertex twice (to much and to few workload)
		return imbalance * vertexImbalanceCosts / 2;
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
