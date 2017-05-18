package mthesis.concurrent_graph.master.vertexmove;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mthesis.concurrent_graph.util.MiscUtil;

public class QueryCluster {

	public final int id;
	public final Set<Integer> queries = new HashSet<>();
	public final Map<Integer, Integer> intersects;
	public long vertices;

	public QueryCluster(int startQuery, long vertices, Map<Integer, Integer> intersects) {
		this.id = startQuery;
		this.vertices = vertices;
		this.intersects = intersects;
		queries.add(startQuery);
	}

	public void mergeOtherCluster(QueryCluster clusterToMerge, Map<Integer, Integer> queryClusterIds, Map<Integer, QueryCluster> clusters) {

		clusters.remove(clusterToMerge.id);
		vertices += clusterToMerge.vertices - clusterToMerge.intersects.get(id);

		// Merge cluster queries
		queries.addAll(clusterToMerge.queries);
		for (int mergedQuery : clusterToMerge.queries) {
			queryClusterIds.put(mergedQuery, id);
		}

		// Merge intersects
		for (Entry<Integer, Integer> intersect : clusterToMerge.intersects.entrySet()) {
			MiscUtil.mapMax(intersects, intersect.getKey(), intersect.getValue());
		}
		for (int clusterQuery : queries) {
			intersects.remove(clusterQuery);
		}

		// Update other intersect references
		for (QueryCluster cluster : clusters.values()) {
			if (cluster.id == id) continue;
			int oldIntersect = MiscUtil.defaultInt(cluster.intersects.remove(clusterToMerge.id));
			if (oldIntersect > 0) {
				MiscUtil.mapMax(cluster.intersects, id, oldIntersect);
			}
		}
	}


	@Override
	public String toString() {
		return id + " " + queries;
	}
}
