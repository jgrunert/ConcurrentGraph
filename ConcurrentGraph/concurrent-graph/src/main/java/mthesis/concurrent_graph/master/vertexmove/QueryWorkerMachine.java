package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.util.MiscUtil;

/**
 * Represents a worker machine with queries active
 *
 * @author Jonas Grunert
 *
 */
public class QueryWorkerMachine {

	// Total number of vertices active in a query (if a vertex is active in n queries it counts as n vertices)
	public int activeVertices;
	public long totalVertices;
	// Active queries on this machine
	public List<QueryVertexChunk> queryChunks;
	// Number of vertices of queries
	public Map<Integer, Integer> queryVertices;


	public QueryWorkerMachine(List<QueryVertexChunk> queryChunks, long totalVertices) {
		super();
		this.queryChunks = queryChunks;
		this.queryVertices = new HashMap<>();
		this.totalVertices = totalVertices;
		activeVertices = 0;
		for (QueryVertexChunk qChunk : queryChunks) {
			activeVertices += qChunk.numVertices;
			for (int query : qChunk.queries) {
				queryVertices.put(query, MiscUtil.defaultInt(queryVertices.get(query)) + qChunk.numVertices);
			}
		}
	}

	public QueryWorkerMachine(List<QueryVertexChunk> queryChunks, int activeVertices, long totalVertices,
			Map<Integer, Integer> queryVertices) {
		super();
		this.queryChunks = queryChunks;
		this.queryVertices = queryVertices;
		this.activeVertices = activeVertices;
		this.totalVertices = totalVertices;
	}

	public QueryWorkerMachine createClone() {
		return new QueryWorkerMachine(new ArrayList<>(queryChunks), activeVertices, totalVertices, new HashMap<>(queryVertices));
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
				totalVertices -= chunk.numVertices;
				for (int query : chunk.queries) {
					queryVertices.put(query, MiscUtil.defaultInt(queryVertices.get(query)) - chunk.numVertices);
				}
				i--;
			}
		}

		return removedQueryChunks;
	}

	/**
	 * Adds QueryVertexChunks to this machine
	 * @param chunksToAdd Chunks to add
	 */
	public void addQueryVertices(List<QueryVertexChunk> chunksToAdd) {
		for (QueryVertexChunk chunk : chunksToAdd) {
			queryChunks.add(chunk);
			activeVertices += chunk.numVertices;
			totalVertices += chunk.numVertices;
			for (int query : chunk.queries) {
				queryVertices.put(query, MiscUtil.defaultInt(queryVertices.get(query)) + chunk.numVertices);
			}
		}
	}

	@Override
	public String toString() {
		return QueryWorkerMachine.class.getSimpleName() + "(" + activeVertices + " activeVertices)";
	}
}
