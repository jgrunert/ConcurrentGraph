package mthesis.concurrent_graph.vertex;

import mthesis.concurrent_graph.writable.BaseWritable;

public class Edge<E extends BaseWritable> {
	public final int NeighborId;
	public E Value;

	public Edge(int neighborId, E value) {
		super();
		NeighborId = neighborId;
		Value = value;
	}
}
