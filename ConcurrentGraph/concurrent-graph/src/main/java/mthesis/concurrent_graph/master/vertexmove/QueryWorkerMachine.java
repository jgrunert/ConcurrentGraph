package mthesis.concurrent_graph.master.vertexmove;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a worker machine with queries active
 *
 * @author Jonas Grunert
 *
 */
public class QueryWorkerMachine {

	// Total number of vertices active in a query (if a vertex is active in n queries it counts as n vertices)
	public int activeVertices;
	// Active queries on this machine
	public Map<Integer, QueryVerticesOnMachine> queries;


	public QueryWorkerMachine(Map<Integer, QueryVerticesOnMachine> queries) {
		super();
		this.queries = queries;
		activeVertices = 0;
		for (QueryVerticesOnMachine q : queries.values()) {
			activeVertices += q.totalVertices;
		}
	}

	public QueryWorkerMachine createClone() {
		Map<Integer, QueryVerticesOnMachine> queriesClone = new HashMap<>(queries);
		for (Entry<Integer, QueryVerticesOnMachine> query : queries.entrySet()) {
			queriesClone.put(query.getKey(), query.getValue().createClone());
		}
		return new QueryWorkerMachine(queriesClone);
	}


	/**
	 * Removes all vertices of a query that are not active in an other query
	 * @return Number of removed vertices
	 */
	public int removeNonIntersecting(int queryId) {
		QueryVerticesOnMachine query = queries.get(queryId);
		if (query == null) {
			// Query not on machine
			return 0;
		}

		if (query.nonIntersectingVertices == query.totalVertices) {
			// Query has no intersections
			queries.remove(queryId);
			return query.totalVertices;
		}
		else {
			// Only remove non intersecting vertices
			int nonIntersecting = query.nonIntersectingVertices;
			query.totalVertices -= nonIntersecting;
			query.nonIntersectingVertices = 0;
			return nonIntersecting;
		}
	}

	/**
	 * Adds non intersecting vertices for a query
	 * @param addVertexCount Number of vertices to add
	 */
	public void addNonIntersecting(int queryId, int addVertexCount) {
		QueryVerticesOnMachine query = queries.get(queryId);
		if (query == null) {
			// Query not on machine, add it
			query = new QueryVerticesOnMachine(addVertexCount, addVertexCount, new HashMap<>());
			queries.put(queryId, query);
		}
		else {
			query.totalVertices += addVertexCount;
			query.nonIntersectingVertices += addVertexCount;
		}
	}
}
