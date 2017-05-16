package mthesis.concurrent_graph.master.vertexmove;

import java.util.Set;

/**
 * Represents an operation moving vertices from one worker machine to another
 * TODO Implement moves not moving all vertices
 *
 * @author Jonas Grunert
 *
 */
@Deprecated
public class VertexMoveOperation {

	public final Set<Integer> QueryChunk;
	public final int ChunkVertices;
	public final int FromMachine;
	public final int ToMachine;


	public VertexMoveOperation(Set<Integer> queryChunk, int chunkSize, int fromMachine, int toMachine) {
		super();
		QueryChunk = queryChunk;
		ChunkVertices = chunkSize;
		FromMachine = fromMachine;
		ToMachine = toMachine;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + FromMachine;
		for (Integer q : QueryChunk) {
			result = prime * result + q;
		}
		result = prime * result + ToMachine;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		VertexMoveOperation other = (VertexMoveOperation) obj;
		if (FromMachine != other.FromMachine) return false;
		if (QueryChunk != other.QueryChunk && QueryChunk != null &&
				!QueryChunk.equals(other.QueryChunk))
			return false;
		if (ToMachine != other.ToMachine) return false;
		return true;
	}
}
