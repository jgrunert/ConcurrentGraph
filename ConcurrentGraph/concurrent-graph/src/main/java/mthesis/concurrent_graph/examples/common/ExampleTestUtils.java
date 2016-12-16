package mthesis.concurrent_graph.examples.common;

import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable;

public class ExampleTestUtils<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable> {

	public WorkerMachine<V, E, M> startWorker(Map<Integer, MachineConfig> allCfg,
			int id, List<Integer> allWorkers, String output,
			JobConfiguration<V, E, M> jobConfig) {
		final WorkerMachine<V, E, M> node = new WorkerMachine<>(allCfg, id, allWorkers, -1, output, jobConfig);
		node.start();
		return node;
	}

	public MasterMachine startMaster(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds,
			String inputFile, String inputPartitionDir, MasterInputPartitioner inputPartitioner,
			MasterOutputEvaluator outputCombiner, String outputDir) {
		final MasterMachine node = new MasterMachine(machines, ownId, workerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir);
		node.start();
		return node;
	}
}
