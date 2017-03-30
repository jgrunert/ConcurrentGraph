package mthesis.concurrent_graph.apps.sssp;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.apputils.MachineClusterConfiguration;
import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPSingleWorkerMain {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [clusterConfigFile] [workerId]");
			return;
		}
		final int workerId = Integer.parseInt(args[2]);
		System.out.println("Starting SSSP SingleWorker " + Configuration.VERSION + " " + workerId);

		Configuration.loadConfig(args[0]);
		final MachineClusterConfiguration config = new MachineClusterConfiguration(args[1]);


		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();

		// Start machines
		System.out.println("Starting machines");
		final RunUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> testUtils = new RunUtils<>();
		testUtils.startWorker(config.AllMachineConfigs, workerId, config.masterId, config.AllWorkerIds, outputDir,
				jobConfig,
				new RoadNetVertexInputReader());
	}
}
