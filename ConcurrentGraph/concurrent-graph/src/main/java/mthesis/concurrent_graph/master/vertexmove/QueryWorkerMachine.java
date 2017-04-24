package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.List;

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
	public List<QueryVertexChunk> queryChunks;


	public QueryWorkerMachine(List<QueryVertexChunk> queryChunks) {
		super();
		this.queryChunks = queryChunks;
		activeVertices = 0;
		for (QueryVertexChunk q : queryChunks) {
			activeVertices += q.numVertices;
		}
	}

	public QueryWorkerMachine(List<QueryVertexChunk> queryChunks, int activeVertices) {
		super();
		this.queryChunks = queryChunks;
		this.activeVertices = activeVertices;
	}

	public QueryWorkerMachine createClone() {
		return new QueryWorkerMachine(new ArrayList<>(queryChunks), activeVertices);
	}


	/**
	 * Removes vertices of a query
	 * @param queryId ID of the query to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @return List of removed QueryVertexChunks
	 */
	public List<QueryVertexChunk> removeQueryVertices(int queryId, boolean moveIntersecting) {
		List<QueryVertexChunk> removedQueryChunks = new ArrayList<>();
		for (int i = 0; i < queryChunks.size(); i++) {
			QueryVertexChunk chunk = queryChunks.get(i);
			if ((moveIntersecting || chunk.queries.size() == 1) && chunk.queries.contains(queryId)) {
				removedQueryChunks.add(chunk);
				queryChunks.remove(i);
				activeVertices -= chunk.numVertices;
				i--;
			}
		}

		return removedQueryChunks;
	}

	/**
	 * Adds query vertices to this machine
	 * @param addVertexCount Number of vertices to add
	 */
	public void addQueryVertices(List<QueryVertexChunk> chunksToAdd) {
		for (QueryVertexChunk chunk : chunksToAdd) {
			queryChunks.add(chunk);
			activeVertices += chunk.numVertices;
		}
	}

	@Override
	public String toString() {
		return QueryWorkerMachine.class.getSimpleName() + "(" + activeVertices + " activeVertices)";
	}
}
