package mthesis.concurrent_graph.vertex;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import mthesis.concurrent_graph.graph.Edge;
import mthesis.concurrent_graph.graph.Graph;
import mthesis.concurrent_graph.writable.BaseWritable;

public class ArrayBackedGraph<V extends BaseWritable, E extends BaseWritable> implements Graph<V, E> {

	// TODO Local ID?
	private final Int2ObjectMap<V> vertexValues = new Int2ObjectOpenHashMap<>();
	private final Int2ObjectMap<Edge<E>[]> vertexEdges = new Int2ObjectOpenHashMap<>();


	public void addVertex(int vertexId, V value, Edge<E>[] edges) {
		vertexValues.put(vertexId, value);
		vertexEdges.put(vertexId, edges);
	}

	public void removeVertex(int vertexId) {
		vertexValues.remove(vertexId);
		vertexEdges.remove(vertexId);
	}


	@Override
	public V getValue(int vertexId) {
		return vertexValues.get(vertexId);
	}

	@Override
	public void setValue(int vertexId, V value) {
		vertexValues.put(vertexId, value);
	}

	@Override
	public Edge<E>[] getOutgoingEdges(int vertexId) {
		return vertexEdges.get(vertexId);
	}

}
