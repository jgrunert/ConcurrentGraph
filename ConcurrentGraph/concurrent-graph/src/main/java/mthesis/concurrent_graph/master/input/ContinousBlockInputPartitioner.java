package mthesis.concurrent_graph.master.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContinousBlockInputPartitioner extends BlockInputPartitioner {
	public ContinousBlockInputPartitioner(int partitionMaxVertices) {
		super(partitionMaxVertices);
	}

	@Override
	public Map<Integer, List<String>> distributePartitions(List<String> partitions, List<Integer> workers) {
		final Map<Integer, List<String>> distributed = new HashMap<>();
		for(final Integer worker : workers) {
			distributed.put(worker,  new ArrayList<>());
		}

		if(partitions.size() < workers.size())
			throw new RuntimeException("Cant have more partitions than workers");

		final int partitionsPerWorker = partitions.size() / workers.size();
		for (int i = 0; i < partitions.size(); i++) {
			distributed.get(workers.get(Math.min(i / partitionsPerWorker, workers.size() - 1))).add(partitions.get(i));
		}

		return distributed;
	}
}
