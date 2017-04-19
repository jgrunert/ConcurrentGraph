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

	public QueryWorkerMachine(Map<Integer, QueryVerticesOnMachine> queries, int activeVertices) {
		super();
		this.queries = queries;
		this.activeVertices = activeVertices;
	}

	public QueryWorkerMachine createClone() {
		Map<Integer, QueryVerticesOnMachine> queriesClone = new HashMap<>(queries);
		for (Entry<Integer, QueryVerticesOnMachine> query : queries.entrySet()) {
			queriesClone.put(query.getKey(), query.getValue().createClone());
		}
		return new QueryWorkerMachine(queriesClone, activeVertices);
	}


	/**
	 * Removes vertices of a query
	 * @param queryId ID of the query to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @return Map<QueryId, Removed vertices and intersections (if any)>
	 */
	public Map<Integer, QueryVerticesOnMachine> removeQueryVertices(int queryId, boolean moveIntersecting) {
		QueryVerticesOnMachine query = queries.get(queryId);
		if (query == null || query.totalVertices == 0) {
			// Query not on machine
			return new HashMap<>();
		}

		Map<Integer, QueryVerticesOnMachine> removed = new HashMap<>();
		if (query.intersectingVertices > 0) {
			// Query has intersections
			if (moveIntersecting) {
				// Move all vertices of this query
				queries.remove(queryId);
				activeVertices -= query.totalVertices;
				removed.put(queryId, query);
				/* Also move vertices of intersected queries.
				 * IMPORTANT: Only removes intersections with this query. If a query has an intersection
				 * with another query and this query at the same time this intersection will
				 * not be removed.
				 */
				for (Entry<Integer, Integer> intersection : query.intersections.entrySet()) {
					int intersectSize = intersection.getValue();
					if (intersectSize <= 0) continue;
					int intersectingQueryId = intersection.getKey();
					QueryVerticesOnMachine intersectingQuery = queries.get(intersectingQueryId);
					if (intersectingQuery == null) continue; // TODO Why?
					if (intersectingQuery.totalVertices <= intersectSize) {
						// Total overlap, remove entire query
						queries.remove(intersectingQueryId);
						activeVertices -= query.totalVertices;
						removed.put(intersectingQueryId, intersectingQuery);
					}
					else {
						// Partial overlap
						intersectingQuery.totalVertices -= intersectSize;
						intersectingQuery.intersectingVertices -= intersectSize;
						intersectingQuery.intersections.remove(queryId);
						Map<Integer, Integer> intersectingIntersections = new HashMap<>();
						intersectingIntersections.put(queryId, intersectSize);
						activeVertices -= intersectSize;
						removed.put(intersection.getKey(), new QueryVerticesOnMachine(intersectSize, intersectingIntersections));
					}
				}
			}
			else {
				// Only remove non intersecting vertices
				int nonIntersecting = query.totalVertices - query.intersectingVertices;
				activeVertices -= nonIntersecting;
				removed.put(queryId, new QueryVerticesOnMachine(nonIntersecting, new HashMap<>()));
				query.totalVertices = query.intersectingVertices;
			}
		}
		else {
			// Query has no intersections, remove entire query
			queries.remove(queryId);
			activeVertices -= query.totalVertices;
			removed.put(queryId, query);
		}
		return removed;
	}

	/**
	 * Adds query vertices to this machine
	 * @param addVertexCount Number of vertices to add
	 */
	public void addQueryVertices(Map<Integer, QueryVerticesOnMachine> queriesToAdd) {
		for (Entry<Integer, QueryVerticesOnMachine> query : queriesToAdd.entrySet()) {
			QueryVerticesOnMachine queryToAdd = query.getValue();
			QueryVerticesOnMachine existingQuery = queries.get(query.getKey());
			if (existingQuery == null) {
				// New query
				queries.put(query.getKey(), queryToAdd);
			}
			else {
				// Merge with existing query
				existingQuery.totalVertices += queryToAdd.totalVertices;
				existingQuery.intersectingVertices += queryToAdd.intersectingVertices;
				for (Entry<Integer, Integer> qAddInters : queryToAdd.intersections.entrySet()) {
					if(existingQuery.intersections.containsKey(qAddInters.getKey())){
						existingQuery.intersections.put(qAddInters.getKey(),
								existingQuery.intersections.get(qAddInters.getKey()) + qAddInters.getValue());
					}
					else {
						existingQuery.intersections.put(qAddInters.getKey(), qAddInters.getValue());
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		return QueryWorkerMachine.class.getSimpleName() + "(" + activeVertices + " activeVertices)";
	}
}
