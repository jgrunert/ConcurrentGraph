package mthesis.concurrent_graph.worker;

import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

/**
 * Local vertex graph of a worker machine
 *
 * @author Jonas Grunert
 *
 */
public class WorkerGraph<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public final Int2ObjectMap<AbstractVertex<V, E, M, Q>> Vertices;


	public WorkerGraph(List<AbstractVertex<V, E, M, Q>> verticesList) {
		Vertices = new Int2ObjectOpenHashMap<>(verticesList.size());
		//		for(AbstractVertex<V, E, M, Q> v : verticesList) {
		//			Vertices.put(key, value)
		//		}
	}
}
