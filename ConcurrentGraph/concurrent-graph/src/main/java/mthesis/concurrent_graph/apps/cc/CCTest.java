package mthesis.concurrent_graph.apps.cc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.MachineConfig;
import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.ContinousBlockInputPartitioner;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.VertexTextInputReader;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.IntWritable;
import mthesis.concurrent_graph.writable.NullWritable;

public class CCTest {

	public static void main(String[] args) throws Exception {
		final int numWorkers = 4;
		final String host = "localhost";
		final int baseControlMsgPort = 23499;
		final String inputPartitionDir = "input";
		final String outputDir = "output";
		//final String inputFile = "../../Data_converted/cctest.txt";
		//final String inputFile = "../../Data_converted/Wiki-Vote.txt";
		final String inputFile = args[0];

		final CCDetectJobConfiguration jobConfig = new CCDetectJobConfiguration();
		final MasterInputPartitioner inputPartitioner = new ContinousBlockInputPartitioner(Integer.parseInt(args[1]));
		final MasterOutputEvaluator<BaseQueryGlobalValues> outputCombiner = new CCOutputWriter();

		// TODO Replace with MachineClusterConfiguration
		final Map<Integer, MachineConfig> allCfg = new HashMap<>();
		final List<Integer> allWorkerIds = new ArrayList<>();
		allCfg.put(-1, new MachineConfig(host, baseControlMsgPort, false));
		for (int i = 0; i < numWorkers; i++) {
			allWorkerIds.add(i);
			allCfg.put(i, new MachineConfig(host, baseControlMsgPort + 1 + i, false));
		}

		System.out.println("Starting machines");
		MasterMachine<BaseQueryGlobalValues> master = null;
		final RunUtils<IntWritable, NullWritable, CCMessageWritable, BaseQueryGlobalValues> testUtils = new RunUtils<>();
		master = testUtils.startMaster(allCfg, -1, allWorkerIds, inputFile, inputPartitionDir, inputPartitioner, outputCombiner, outputDir,
				jobConfig);

		final List<WorkerMachine<IntWritable, NullWritable, CCMessageWritable, BaseQueryGlobalValues>> workers = new ArrayList<>();
		for (int i = 0; i < numWorkers; i++) {
			workers.add(testUtils.startWorker(allCfg, i, allWorkerIds, outputDir, jobConfig, new VertexTextInputReader<>()));
		}

		if (master != null) master.startQuery(new BaseQueryGlobalValues(0));
	}
}
