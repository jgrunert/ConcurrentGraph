package mthesis.concurrent_graph.examples.sssp;

import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.examples.common.ExampleTestUtils;
import mthesis.concurrent_graph.examples.common.MachineClusterConfiguration;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.worker.WorkerMachine;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPTest {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [inputFile] [partitionPerWorker]");
			return;
		}
		MachineClusterConfiguration config = new MachineClusterConfiguration(args[0]);
		final String inputFile = args[1];
		final int partitionPerWorker = Integer.parseInt(args[2]);

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();
		// final MasterInputPartitioner inputPartitioner = new
		// ContinousBlockInputPartitioner(partitionSize);
		final MasterInputPartitioner inputPartitioner = new RoadNetInputPartitioner(partitionPerWorker);
		final MasterOutputEvaluator<SSSPQueryValues> outputCombiner = new SSSPOutputEvaluator();

		// Start machines
		System.out.println("Starting machines");
		final ExampleTestUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> testUtils = new ExampleTestUtils<>();
		MasterMachine<SSSPQueryValues> master = null;
		if (config.StartOnThisMachine.get(config.masterId))
			master = testUtils.startMaster(config.AllMachineConfigs, config.masterId, config.AllWorkerIds, inputFile,
					inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig);

		final List<WorkerMachine<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues>> workers = new ArrayList<>();
		for (int i = 0; i < config.AllWorkerIds.size(); i++) {
			if (config.StartOnThisMachine.get(config.AllWorkerIds.get(i))) workers
			.add(testUtils.startWorker(config.AllMachineConfigs, i, config.AllWorkerIds, outputDir, jobConfig,
					new RoadNetVertexInputReader()));
		}

		// Start query
		if (master != null) {
			// System.out.println("Starting query test");
			// Random rd = new Random(0);
			// for (int i = 0; i < 100; i++) {
			// int from = rd.nextInt(1090863);
			// int to = rd.nextInt(1090863);
			// master.startQuery(new SSSPQueryValues(i, from, to, 100));
			// }

			int queryIndex = 0;

			// Very short ST-HBF->ST-Airport. Test query "0".
			// Ca 3.5s for local4, no vertexmove, without sysout, on PC and 12s on laptop
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 7653486));

			// Very short Meersburg->Pfullendorf. Test query "1"
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337));

			// Short Heidelberg->Heilbronn. Test query "2"
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011));

			// Short Heidelberg->Heilbronn. Test query "3"
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011));


			// Short RT->ST
			// Ca ?s for local4, no vertexmove, without sysout, on PC and 13s on laptop
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));

			//			// Big query through BW, Ludwigshafen->Heilbronn
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2942985, 6663036));
			//
			//			// Short Mengen->Saulgau
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3080719, 609074));

			//			// Medium PF->HB
			// Ca ?s for local4, no vertexmove, without sysout, on PC and 34s on laptop
			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821));

			//			// Short TU->RT
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 4982624, 3627927));
			//
			// Very short ST-Echterdingen->ST-HBF
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 4304982, 7031164));
			//
			// Short RT->ST
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 7894832, 3184057));
			//
			//			// Medium UL->ST
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 7311538, 589587));
			//
			//			// Short RT->TU
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3627927, 4982624));

			// Short ST-HBF->TU
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 4982624));

			master.waitForAllQueriesFinish();
			master.stop();
		}
	}
}
