package mthesis.concurrent_graph.examples.common;

import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable;

public class ExampleTestUtils<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, G extends BaseQueryGlobalValues> {

	public WorkerMachine<V, E, M, G> startWorker(Map<Integer, MachineConfig> allCfg, int id, List<Integer> allWorkers, String output,
			JobConfiguration<V, E, M, G> jobConfig) {
		final WorkerMachine<V, E, M, G> node = new WorkerMachine<>(allCfg, id, allWorkers, -1, output, jobConfig);
		node.start();
		return node;
	}

	public MasterMachine<G> startMaster(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, String inputFile,
			String inputPartitionDir, MasterInputPartitioner inputPartitioner, MasterOutputEvaluator<G> outputCombiner, String outputDir,
			JobConfiguration<V, E, M, G> jobConfig) {
		return startMaster(machines, ownId, workerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig,
				jobConfig.getGlobalValuesFactory().createDefault());
	}

	public MasterMachine<G> startMaster(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, String inputFile,
			String inputPartitionDir, MasterInputPartitioner inputPartitioner, MasterOutputEvaluator<G> outputCombiner, String outputDir,
			JobConfiguration<V, E, M, G> jobConfig, G jobQuery) {
		final MasterMachine<G> node = new MasterMachine<G>(machines, ownId, workerIds, inputFile, inputPartitionDir, inputPartitioner,
				outputCombiner, outputDir, jobConfig.getGlobalValuesFactory(), jobQuery);
		node.start();
		return node;
	}
}
