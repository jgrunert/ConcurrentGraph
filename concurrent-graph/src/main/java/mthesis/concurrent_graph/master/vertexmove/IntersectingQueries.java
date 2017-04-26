package mthesis.concurrent_graph.master.vertexmove;

import java.util.HashSet;

import mthesis.concurrent_graph.util.Pair;

/**
 * Represents a set of intersection queries on a worker machine
 *
 * @author Jonas Grunert
 *
 */
public class IntersectingQueries {

	/** Set of <QueryId, NumVertices> */
	public final HashSet<Pair<Integer, Integer>> QueryVertices = new HashSet<>();
}
