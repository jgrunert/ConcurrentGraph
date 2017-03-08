package mthesis.concurrent_graph.apputils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.BaseVertexInputReader;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable;

public class RunUtil<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public MasterMachine<Q> startSetup(MachineClusterConfiguration config,
			String inputFile, String inputPartitionDir, MasterInputPartitioner inputPartitioner,
			MasterOutputEvaluator<Q> outputCombiner, String outputDir,
			JobConfiguration<V, E, M, Q> jobConfig, BaseVertexInputReader<V, E, M, Q> vertexReader) {
		MasterMachine<Q> master = null;
		if (config.StartOnThisMachine.get(config.masterId))
			master = this.startMaster(config.AllMachineConfigs, config.masterId, config.AllWorkerIds, inputFile,
					inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		final List<WorkerMachine<V, E, M, Q>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			if (config.StartOnThisMachine.get(config.AllWorkerIds.get(i))) workers
					.add(this.startWorker(config.AllMachineConfigs, i, config.AllWorkerIds, outputDir, jobConfig,
							vertexReader));
		}

		return master;
	}

	public WorkerMachine<V, E, M, Q> startWorker(Map<Integer, MachineConfig> allCfg, int id, List<Integer> allWorkers, String output,
			JobConfiguration<V, E, M, Q> jobConfig, BaseVertexInputReader<V, E, M, Q> vertexReader) {
		final WorkerMachine<V, E, M, Q> node = new WorkerMachine<>(allCfg, id, allWorkers, -1, output, jobConfig, vertexReader);
		Thread workerStartThread = new Thread(new Runnable() {

			@Override
			public void run() {
				node.start();
			}
		});
		workerStartThread.setName("workerStartThread");
		workerStartThread.start();
		return node;
	}

	public MasterMachine<Q> startMaster(Map<Integer, MachineConfig> machines, int ownId, List<Integer> workerIds, String inputFile,
			String inputPartitionDir, MasterInputPartitioner inputPartitioner, MasterOutputEvaluator<Q> outputCombiner, String outputDir,
			JobConfiguration<V, E, M, Q> jobConfig) {
		final MasterMachine<Q> node = new MasterMachine<Q>(machines, ownId, workerIds, inputFile, inputPartitionDir, inputPartitioner,
				outputCombiner, outputDir, jobConfig.getGlobalValuesFactory());
		Thread masterStartThread = new Thread(new Runnable() {

			@Override
			public void run() {
				node.start();
			}
		});
		masterStartThread.setName("masterStartThread");
		masterStartThread.start();
		return node;
	}
}
