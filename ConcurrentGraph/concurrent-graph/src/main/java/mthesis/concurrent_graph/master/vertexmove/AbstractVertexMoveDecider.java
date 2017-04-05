package mthesis.concurrent_graph.master.vertexmove;

import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.master.MasterQuery;

/**
 * Abstract base class for VertexMove deciders.
 * Decides if and which vertices to move. Returns null if no move at all or a VertexMoveDecision.
 *
 * @author Jonas Grunert
 *
 */
public abstract class AbstractVertexMoveDecider<Q extends BaseQuery> {

	public abstract VertexMoveDecision decide(List<Integer> workerIds, Map<Integer, MasterQuery<Q>> activeQueries,
			Map<Integer, Map<Integer, Integer>> actQueryWorkerActiveVerts,
			Map<Integer, Map<Integer, Map<Integer, Integer>>> actQueryWorkerIntersects);
}
