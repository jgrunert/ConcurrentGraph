package mthesis.concurrent_graph.apps.shortestpath.partitioning;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class LinearDeterministicGreedy {

	private int k;	// the number of partitions
	private HashMap<Integer,Set<Integer>> P; // partition id --> set of assigned vertices
	private double C; // the memory constraint, i.e., maximal number of vertices per partition
	
	public LinearDeterministicGreedy(int numberOfPartitions, int numberOfVertices) {
		this.k = numberOfPartitions;
		this.P = new HashMap<Integer,Set<Integer>>();
		for (int i=0; i<k; i++) {
			this.P.put(i, new HashSet<Integer>());
		}
		this.C = (double)numberOfVertices / (double)k;
		

	}
	
	public int getPartitionID(int vertexID, Set<Integer> neighbors) {
		int bestPartitionID = 0;
		double bestScore = -1;
		
		// determine partition with highest score for vertex
		for (int i=0; i<k; i++) {
			double score = this.getLDGScore(vertexID, neighbors, i);
			if (score>bestScore) {
				bestScore = score;
				bestPartitionID = i;
			} else if (score==bestScore) {
				// tiebreaker: Balanced
				if (P.get(bestPartitionID).size()>P.get(i).size()) {
					// i is smaller partition -> prefer i
					bestPartitionID = i;
				}
			}
		}
		
		// assign vertex to partition
		this.P.get(bestPartitionID).add(vertexID);
		
		return bestPartitionID;
	}
	
	
	private double getLDGScore(
			int vertexID,
			Set<Integer> neighbors,
			int partitionID) {
		
		// calculate intersection between neighbors and vertices on partition
		Set<Integer> part = this.P.get(partitionID);
		neighbors.retainAll(part);
		
		// calculate score as given in SIGKDD'12 [Stanton2012Streaming]
		double score = neighbors.size() * (1 - (double)part.size() / C);
		return score;
	}
	
	@Override
	public String toString() {
		String s = "";
		for (int i=0; i<k; i++) {
			s += "Partition " + i + " has " + P.get(i).size() + " vertices.\n";
		}
		return s;
	}
}
