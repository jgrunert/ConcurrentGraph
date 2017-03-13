package mthesis.concurrent_graph.graph;

import mthesis.concurrent_graph.writable.BaseWritable;

public class Edge<E extends BaseWritable> {

	public int NeighborId;
	public E Value;

	public Edge() {

	}

	public Edge(int neighborId, E value) {
		super();
		NeighborId = neighborId;
		Value = value;
	}

	@Override
	public String toString() {
		return NeighborId + "(" + valueToString() + ")";
	}

	private String valueToString() {
		if (Value == null)
			return "";
		return Value.getString();
	}
}
