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
	// Total number of vertices of this query that are active in another query
	public int intersectingVertices;
	// Vertices of this query shared with other query
	public Map<Integer, Integer> intersections;

	public QueryVerticesOnMachine(int totalVertices, Map<Integer, Integer> intersections) {
		super();
		this.totalVertices = totalVertices;
		this.intersectingVertices = 0;
		for (Integer inters : intersections.values()) {
			this.intersectingVertices += inters;
		}
		this.intersections = intersections;
	}

	public QueryVerticesOnMachine(int totalVertices, int intersectingVertices, Map<Integer, Integer> intersections) {
		super();
		this.totalVertices = totalVertices;
		this.intersectingVertices = intersectingVertices;
		this.intersections = intersections;
	}

	public QueryVerticesOnMachine createClone() {
		return new QueryVerticesOnMachine(totalVertices, intersectingVertices, new HashMap<>(intersections));
	}
}
