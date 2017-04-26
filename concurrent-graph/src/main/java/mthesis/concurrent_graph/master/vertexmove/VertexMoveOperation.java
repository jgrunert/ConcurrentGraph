package mthesis.concurrent_graph.master.vertexmove;

/**
 * Represents an operation moving vertices from one worker machine to another
 * TODO Implement moves not moving all vertices
 *
 * @author Jonas Grunert
 *
 */
public class VertexMoveOperation {

	public final int QueryId;
	public final int FromMachine;
	public final int ToMachine;


	public VertexMoveOperation(int queryId, int fromMachine, int toMachine) {
		super();
		QueryId = queryId;
		FromMachine = fromMachine;
		ToMachine = toMachine;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + FromMachine;
		result = prime * result + QueryId;
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
		if (QueryId != other.QueryId) return false;
		if (ToMachine != other.ToMachine) return false;
		return true;
	}
}
