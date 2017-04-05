package mthesis.concurrent_graph.apputils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.BaseVertexInputReader;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.BaseWritable;

public class RunUtils<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	public MasterMachine<Q> startSetup(String configFile, boolean extraJvmPerWorker,
			String inputFile, String inputPartitionDir, MasterInputPartitioner inputPartitioner,
			MasterOutputEvaluator<Q> outputCombiner, String outputDir,
			JobConfiguration<V, E, M, Q> jobConfig, BaseVertexInputReader<V, E, M, Q> vertexReader) {
		MachineClusterConfiguration config = new MachineClusterConfiguration(configFile);
		MasterMachine<Q> master = null;
		master = this.startMaster(config.AllMachineConfigs, config.masterId, config.AllWorkerIds, inputFile,
				inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		final List<WorkerMachine<V, E, M, Q>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			int workerId = config.AllWorkerIds.get(i);
			if (extraJvmPerWorker) {
				try {
					Process proc = Runtime.getRuntime().exec("java -jar single_worker.jar " + configFile + " " + workerId);
					//						InputStream in = proc.getInputStream();
					//						InputStream err = proc.getErrorStream();
					StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream());
					StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream());
					errorGobbler.start();
					outputGobbler.start();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			else {
				workers.add(this.startWorker(config.AllMachineConfigs, workerId, config.masterId, config.AllWorkerIds,
						outputDir, jobConfig,
						vertexReader));
			}
		}

		return master;
	}

	public WorkerMachine<V, E, M, Q> startWorker(Map<Integer, MachineConfig> allCfg, int id, int masterId,
			List<Integer> allWorkers, String output,
			JobConfiguration<V, E, M, Q> jobConfig, BaseVertexInputReader<V, E, M, Q> vertexReader) {
		final WorkerMachine<V, E, M, Q> node = new WorkerMachine<>(allCfg, id, allWorkers, masterId, output, jobConfig,
				vertexReader);
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


	private static class StreamGobbler extends Thread {

		InputStream is;

		// reads everything from is until empty.
		StreamGobbler(InputStream is) {
			this.is = is;
		}

		@Override
		public void run() {
			try {
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String line = null;
				while ((line = br.readLine()) != null)
					System.out.println(line);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}
}
