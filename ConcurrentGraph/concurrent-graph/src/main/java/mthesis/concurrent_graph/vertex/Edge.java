package mthesis.concurrent_graph.vertex;

import mthesis.concurrent_graph.writable.BaseWritable;

public class Edge<E extends BaseWritable> {
	public final int TargetVertexId;
	public E Value;

	public Edge(int neighborId, E value) {
		super();
		TargetVertexId = neighborId;
		Value = value;
	}

	@Override
	public String toString() {
		return TargetVertexId + "(" + valueToString() + ")";
	}

	private String valueToString() {
		if(Value == null)
			return "";
		return Value.GetString();
	}
}
