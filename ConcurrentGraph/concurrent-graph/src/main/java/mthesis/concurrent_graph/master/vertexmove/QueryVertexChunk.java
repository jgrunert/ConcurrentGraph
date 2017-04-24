package mthesis.concurrent_graph.master.vertexmove;

import it.unimi.dsi.fastutil.ints.IntSet;


/**
 * Represents a chunk of vertices, originated at a given machine.
 * Can be moved between machines.
 *
 * @author Jonas Grunert
 *
 */
public class QueryVertexChunk {

	public final IntSet queries;
	public final int numVertices;
	public final int homeMachine;

	public QueryVertexChunk(IntSet queries, int numVertices, int homeMachine) {
		this.queries = queries;
		this.numVertices = numVertices;
		this.homeMachine = homeMachine;
	}
}
