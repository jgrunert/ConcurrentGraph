package mthesis.concurrent_graph.apps.sssp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.apputils.MachineClusterConfiguration;
import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPSingleWorkerMain {

	private static final Logger logger = LoggerFactory.getLogger(SSSPSingleWorkerMain.class);


	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [clusterConfigFile] [workerId]");
			return;
		}
		final int workerId = Integer.parseInt(args[2]);
		logger.info("Starting SSSP SingleWorker " + Configuration.VERSION + " " + workerId);

		Configuration.loadConfig(args[0]);
		final MachineClusterConfiguration config = new MachineClusterConfiguration(args[1]);


		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();

		// Start machines
		final RunUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> testUtils = new RunUtils<>();
		testUtils.startWorker(config.AllMachineConfigs, workerId, config.masterId, config.AllWorkerIds, outputDir,
				jobConfig,
				new RoadNetVertexInputReader());
	}
}
