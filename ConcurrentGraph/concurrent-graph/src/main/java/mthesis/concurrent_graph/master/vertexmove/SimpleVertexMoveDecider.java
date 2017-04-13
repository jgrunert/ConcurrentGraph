//package mthesis.concurrent_graph.master.vertexmove;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import mthesis.concurrent_graph.BaseQuery;
//import mthesis.concurrent_graph.Configuration;
//import mthesis.concurrent_graph.communication.Messages;
//import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage;
//import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage;
//import mthesis.concurrent_graph.master.MasterQuery;
//import mthesis.concurrent_graph.util.MiscUtil;
//
//public class SimpleVertexMoveDecider<Q extends BaseQuery> extends AbstractVertexMoveDecider<Q> {
//
//	private static final double tolerableAvVariance = 0.2;
//	private long vertexBarrierMoveLastTime = System.currentTimeMillis();
//
//
//	@Override
//	public VertexMoveDecision decide(List<Integer> workerIds, Map<Integer, MasterQuery<Q>> activeQueries,
//			Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts,
//			Map<Integer, Map<Integer, Map<Integer, Integer>>> actQueryWorkerIntersects) {
//
//		if (!Configuration.VERTEX_BARRIER_MOVE_ENABLED
//				|| (System.currentTimeMillis() - vertexBarrierMoveLastTime) < Configuration.VERTEX_BARRIER_MOVE_INTERVAL)
//			return null;
//
//		// Total worker load
//		// TODO Diagram output
//		Map<Integer, Integer> workersActiveVerts = new HashMap<>(actQueryWorkerActiveVerts.size());
//		for (Entry<Integer, Map<Integer, Integer>> queryWorkers : actQueryWorkerActiveVerts.entrySet()) {
//			for (Entry<Integer, Integer> workerActVerts : queryWorkers.getValue().entrySet()) {
//				Integer workerVerts = MiscUtil.defaultInt(workersActiveVerts.get(workerActVerts.getKey()));
//				workersActiveVerts.put(workerActVerts.getKey(), workerVerts + workerActVerts.getValue());
//			}
//		}
//
//		int workerActVertAvg = 0;
//		for (Integer workerActVerts : workersActiveVerts.values()) {
//			workerActVertAvg += workerActVerts;
//		}
//		workerActVertAvg /= workersActiveVerts.size();
//
//
//		boolean anyMoves = false;
//		Map<Integer, List<SendQueryVerticesMessage>> workerVertSendMsgs = new HashMap<>();
//		Map<Integer, List<ReceiveQueryVerticesMessage>> workerVertRecvMsgs = new HashMap<>();
//
//		for (int workerId : workerIds) {
//			workerVertSendMsgs.put(workerId, new ArrayList<>());
//			workerVertRecvMsgs.put(workerId, new ArrayList<>());
//		}
//
//		// Calculate query moves
//		for (MasterQuery<Q> activeQuery : activeQueries.values()) {
//			Map<Integer, Integer> queriesWorkerActiveVerts = actQueryWorkerActiveVerts.get(activeQuery.BaseQuery.QueryId);
//			Map<Integer, Integer> queriesWorkerIntersectsSum = new HashMap<>(queriesWorkerActiveVerts.size());
//			for (Entry<Integer, Map<Integer, Integer>> wIntersects : actQueryWorkerIntersects.get(activeQuery.BaseQuery.QueryId)
//					.entrySet()) {
//				int intersectSum = 0;
//				for (Integer intersect : wIntersects.getValue().values()) {
//					intersectSum += intersect;
//				}
//				queriesWorkerIntersectsSum.put(wIntersects.getKey(), intersectSum);
//				// TODO Testcode
//				//				if (intersectSum > 0) {
//				//					System.err.println("INTERSECT " + wIntersects.getKey() + " " + wIntersects);
//				//				}
//			}
//
//			// TODO Just a simple test algorithm:
//			// Move all other vertices to worker with most active vertices and sub-average active vertices
//			List<Integer> sortedWorkers = new ArrayList<>(MiscUtil.sortByValueInverse(queriesWorkerActiveVerts).keySet());
//			int recvWorkerIndexTmp = 0;
//			while (workersActiveVerts.get(sortedWorkers.get(recvWorkerIndexTmp)) > workerActVertAvg) {
//				recvWorkerIndexTmp++;
//			}
//			int receivingWorker = sortedWorkers.get(recvWorkerIndexTmp);
//			sortedWorkers.remove(recvWorkerIndexTmp);
//
//			for (int i = 0; i < sortedWorkers.size(); i++) {
//				int workerId = sortedWorkers.get(i);
//				double workerVaDiff = workerActVertAvg > 0
//						? (double) (workersActiveVerts.get(workerId) - workerActVertAvg) / workerActVertAvg
//						: 0;
//				// Only move vertices from workers with active, query-exclusive vertices and enough active vertices
//				if (queriesWorkerActiveVerts.get(workerId) > 0 && queriesWorkerIntersectsSum.get(workerId) == 0 &&
//						workerVaDiff > -tolerableAvVariance) {
//					// TODO Modify queriesWorkerActiveVerts according to movement
//					anyMoves = true;
//					workerVertSendMsgs.get(workerId).add(
//							Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage.newBuilder()
//									.setQueryId(activeQuery.BaseQuery.QueryId)
//									.setMoveToMachine(receivingWorker).setMaxMoveCount(Integer.MAX_VALUE)
//									.build());
//					workerVertRecvMsgs.get(receivingWorker).add(
//							Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage.newBuilder()
//									.setQueryId(activeQuery.BaseQuery.QueryId)
//									.setReceiveFromMachine(workerId).build());
//				}
//			}
//		}
//
//		if (!anyMoves) {
//			return null;
//		}
//		else {
//			vertexBarrierMoveLastTime = System.currentTimeMillis();
//			return new VertexMoveDecision(workerVertSendMsgs, workerVertRecvMsgs);
//		}
//	}
//
//}
