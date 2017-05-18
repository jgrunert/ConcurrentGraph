package mthesis.concurrent_graph.master.vertexmove;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.util.MiscUtil;

/**
 * Represents a worker machine with queries active
 *
 * @author Jonas Grunert
 *
 */
public class QueryWorkerMachine {

	// Total number of vertices active in a query (if a vertex is active in n queries it counts as n vertices)
	public long chunkVertices;
	public long totalVertices;
	// Active queries on this machine
	public List<QueryVertexChunk> queryChunks;
	// Number of vertices of queries
	public Map<Integer, Long> queryVertices;


	public QueryWorkerMachine(List<QueryVertexChunk> queryChunks, long totalVertices) {
		super();
		this.queryChunks = queryChunks;
		this.queryVertices = new HashMap<>();
		this.totalVertices = totalVertices;
		chunkVertices = 0;
		for (QueryVertexChunk qChunk : queryChunks) {
			chunkVertices += qChunk.numVertices;
			for (int query : qChunk.queries) {
				queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) + qChunk.numVertices);
			}
		}
	}

	public QueryWorkerMachine(List<QueryVertexChunk> queryChunks, long chunkVertices, long totalVertices,
			Map<Integer, Long> queryVertices) {
		super();
		this.queryChunks = queryChunks;
		this.queryVertices = queryVertices;
		this.chunkVertices = chunkVertices;
		this.totalVertices = totalVertices;
	}

	public QueryWorkerMachine createClone() {
		return new QueryWorkerMachine(new ArrayList<>(queryChunks), chunkVertices, totalVertices, new HashMap<>(queryVertices));
	}


	/**
	 * Removes all vertices of a query
	 * @param queryId ID of the query to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @return List of removed QueryVertexChunks
	 */
	public List<QueryVertexChunk> removeAllQueryVertices(int queryId, boolean moveIntersecting) {
		List<QueryVertexChunk> removedQueryChunks = new ArrayList<>();
		for (int i = 0; i < queryChunks.size(); i++) {
			QueryVertexChunk chunk = queryChunks.get(i);
			if ((moveIntersecting || chunk.queries.size() == 1) && chunk.queries.contains(queryId)) {
				removedQueryChunks.add(chunk);
				queryChunks.remove(i);
				chunkVertices -= chunk.numVertices;
				totalVertices -= chunk.numVertices;
				for (int query : chunk.queries) {
					queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) - chunk.numVertices);
				}
				i--;
			}
		}
		return removedQueryChunks;
	}

	/**
	 * Removes all vertices of a query
	 * @param queryId ID of the query to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @movingTo Machine moving vertex to after removal
	 * @param localQueries Local queries, can move only to their largest partition.
	 * @return List of removed QueryVertexChunks
	 */
	public List<QueryVertexChunk> removeAllQueryVertices(int workerId, int queryId, int movingTo, Map<Integer, Integer> localQueries) {
		List<QueryVertexChunk> removedQueryChunks = new ArrayList<>();
		for (int i = 0; i < queryChunks.size(); i++) {
			QueryVertexChunk chunk = queryChunks.get(i);
			if (chunk.queries.contains(queryId)) {
				for (Integer chunkQuery : chunk.queries) {
					Integer chunkLocal = localQueries.get(chunkQuery);
					//if (chunkLocal != null && !chunkLocal.equals(movingTo))
					if (chunkLocal != null && chunkLocal.equals(workerId))
						continue;
				}

				removedQueryChunks.add(chunk);
				queryChunks.remove(i);
				chunkVertices -= chunk.numVertices;
				totalVertices -= chunk.numVertices;
				for (int query : chunk.queries) {
					queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) - chunk.numVertices);
				}
				i--;
			}
		}
		return removedQueryChunks;
	}


	/**
	 * Removes all vertices of chunks with a given clusterId
	 * @param clusterId ID of the cluster to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @movingTo Machine moving vertex to after removal
	 * @param localQueries Local queries, can move only to their largest partition.
	 * @return List of removed QueryVertexChunks
	 */
	public List<QueryVertexChunk> removeAllClusterVertices(int workerId, int clusterId, int movingTo) {
		List<QueryVertexChunk> removedQueryChunks = new ArrayList<>();
		for (int i = 0; i < queryChunks.size(); i++) {
			QueryVertexChunk chunk = queryChunks.get(i);
			if (chunk.clusters.contains(clusterId)) {
				//				for (Integer chunkQuery : chunk.queries) {
				//					Integer chunkLocal = localQueries.get(chunkQuery);
				//					//if (chunkLocal != null && !chunkLocal.equals(movingTo))
				//					if (chunkLocal != null && chunkLocal.equals(workerId)) continue;
				//				}

				removedQueryChunks.add(chunk);
				queryChunks.remove(i);
				chunkVertices -= chunk.numVertices;
				totalVertices -= chunk.numVertices;
				for (int query : chunk.queries) {
					queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) - chunk.numVertices);
				}
				i--;
			}
		}
		return removedQueryChunks;
	}

	/**
	 * Removes vertices of a query chunk
	 * @param queryId ID of the query to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @return Removed QueryVertexChunks or NULL if not existing
	 */
	public List<QueryVertexChunk> removeAllQueryChunkVertices(IntSet chunkQueries) {
		List<QueryVertexChunk> removedQueryChunks = new ArrayList<>();
		for (int i = 0; i < queryChunks.size(); i++) {
			QueryVertexChunk chunk = queryChunks.get(i);
			if (chunk.queries.equals(chunkQueries)) {
				removedQueryChunks.add(chunk);
				queryChunks.remove(i);
				chunkVertices -= chunk.numVertices;
				totalVertices -= chunk.numVertices;
				for (int query : chunk.queries) {
					queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) - chunk.numVertices);
				}
				i--;
			}
		}
		return removedQueryChunks;
	}

	/**
	 * Removes vertices of a query chunk
	 * @param queryId ID of the query to remove
	 * @param moveIntersecting If false only moves vertices that are not active in an other query
	 * @return Removed QueryVertexChunks or NULL if not existing
	 */
	public boolean removeSingleQueryChunkVertices(QueryVertexChunk queryChunk) {
		if (queryChunks.remove(queryChunk)) {
			chunkVertices -= queryChunk.numVertices;
			totalVertices -= queryChunk.numVertices;
			for (int query : queryChunk.queries) {
				queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) - queryChunk.numVertices);
			}
			return true;
		}
		else {
			return false;
		}
	}


	/**
	 * Adds QueryVertexChunks to this machine
	 * @param chunksToAdd Chunks to add
	 */
	public void addQueryChunk(QueryVertexChunk chunk) {
		queryChunks.add(chunk);
		chunkVertices += chunk.numVertices;
		totalVertices += chunk.numVertices;
		for (int query : chunk.queries) {
			queryVertices.put(query, MiscUtil.defaultLong(queryVertices.get(query)) + chunk.numVertices);
		}
	}


	/** Returns query with least active vertices on this machine */
	public QueryVertexChunk getSmallestChunk() {
		QueryVertexChunk minChunk = null;
		long minChunkSize = Long.MAX_VALUE;
		for (QueryVertexChunk chunk : queryChunks) {
			if (chunk.numVertices > 0 && chunk.numVertices < minChunkSize) {
				minChunkSize = chunk.numVertices;
				minChunk = chunk;
			}
		}
		return minChunk;
	}


	public long getNumVerticesWithQueries(IntSet queries) {
		long verts = 0;
		for (Entry<Integer, Long> qVerts : queryVertices.entrySet()) {
			if (queries.contains(qVerts.getKey()))
				verts += qVerts.getValue();
		}
		return verts;
	}


	@Override
	public String toString() {
		return QueryWorkerMachine.class.getSimpleName() + "(" + totalVertices + " totalVertices " + chunkVertices + " chunkVertices)";
	}
}
