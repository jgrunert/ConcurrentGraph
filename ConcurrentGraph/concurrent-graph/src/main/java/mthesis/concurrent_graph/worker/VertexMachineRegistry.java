package mthesis.concurrent_graph.worker;

import java.util.HashMap;
import java.util.Map;

public class VertexMachineRegistry {

	//private int superstepNow;
	private final Map<Integer, Integer> vertexMachineRegistry = new HashMap<>();


	//	public void NextSuperstep(int superstepNo) {
	//		superstepNow = superstepNo;
	//		// TODO Free unused entries?
	//	}

	public void addEntry(int vertexId, int machineId) {
		vertexMachineRegistry.put(vertexId, machineId);
	}

	/**
	 * Looks up a verte/machine entry. Returns it or NULL if no entry for this vertex.
	 */
	public Integer lookupEntry(int vertexId) {
		return vertexMachineRegistry.get(vertexId);
	}
}
