package mthesis.concurrent_graph.master.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoundRobinBlockInputPartitioner extends BlockInputPartitioner {
	public RoundRobinBlockInputPartitioner(int partitionMaxVertices) {
		super(partitionMaxVertices);
	}

	@Override
	public Map<Integer, List<String>> distributePartitions(List<String> partitions, List<Integer> workers) {
		final Map<Integer, List<String>> distributed = new HashMap<>();
		for(final Integer worker : workers) {
			distributed.put(worker,  new ArrayList<>());
		}

		for(int i = 0; i < partitions.size(); i++) {
			distributed.get(workers.get(i % workers.size())).add(partitions.get(i));
		}

		return distributed;
	}
}
