package mthesis.concurrent_graph.master.vertexmove;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the vertices belonging to a query on a machine.
 *
 * @author Jonas Grunert
 *
 */
public class QueryVerticesOnMachine {

	// All vertices of this query on machine
	public int totalVertices;
	// Vertices of this query that are not active in another query
	public int nonIntersectingVertices;
	// Vertices of this query shared with other query
	public Map<Integer, Integer> intersections;

	public QueryVerticesOnMachine(int totalVertices, Map<Integer, Integer> intersections) {
		super();
		this.totalVertices = totalVertices;
		this.nonIntersectingVertices = totalVertices;
		for (Integer inters : intersections.values()) {
			this.nonIntersectingVertices -= inters;
		}
		this.intersections = intersections;
	}

	public QueryVerticesOnMachine(int totalVertices, int nonIntersectingVertices, Map<Integer, Integer> intersections) {
		super();
		this.totalVertices = totalVertices;
		this.nonIntersectingVertices = nonIntersectingVertices;
		this.intersections = intersections;
	}

	public QueryVerticesOnMachine createClone() {
		return new QueryVerticesOnMachine(totalVertices, nonIntersectingVertices, new HashMap<>(intersections));
	}
}
