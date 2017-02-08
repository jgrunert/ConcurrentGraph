package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class VertexMachineRegistry {

	//private int superstepNow;
	private final Map<Integer, Integer> vertexMachineRegistry = new HashMap<>();


	//	public void NextSuperstep(int superstepNo) {
	//		superstepNow = superstepNo;
	//		// TODO Free unused entries?
	//	}

	/**
	 * Adds a vertex->machine mapping
	 * @return True if mapping is new, false if a mapping for this vertex was already known
	 */
	public synchronized boolean addEntry(int vertexId, int machineId) {
		return vertexMachineRegistry.put(vertexId, machineId) == null;
	}

	/**
	 * Remove a vertex->machine mapping
	 * @return the previous value associated with key, or null if there was no mapping for key.
	 */
	public synchronized Integer removeEntry(int vertexId) {
		return vertexMachineRegistry.remove(vertexId);
	}

	/**
	 * Removes multiple vertex->machine mappings
	 * @return Number of removed vertices
	 */
	public synchronized int removeEntries(Collection<Integer> vertexIds) {
		int removed = 0;
		for (Integer vertexId : vertexIds) {
			if (vertexMachineRegistry.remove(vertexId) != null) removed++;
		}
		return removed;
	}

	/**
	 * Looks up a verte/machine entry. Returns it or NULL if no entry for this vertex.
	 */
	public synchronized Integer lookupEntry(int vertexId) {
		return vertexMachineRegistry.get(vertexId);
	}


	public synchronized int getRegistrySize() {
		return vertexMachineRegistry.size();
	}
}
