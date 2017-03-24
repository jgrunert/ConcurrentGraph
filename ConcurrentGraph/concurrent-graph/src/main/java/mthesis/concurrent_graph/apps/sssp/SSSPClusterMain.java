package mthesis.concurrent_graph.apps.sssp;

import mthesis.concurrent_graph.apputils.RunUtils;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.MasterOutputEvaluator;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.writable.DoubleWritable;

public class SSSPClusterMain {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: [configFile] [inputFile] [partitionPerWorker]");
			return;
		}
		final String inputFile = args[1];
		final int partitionPerWorker = Integer.parseInt(args[2]);

		final String inputPartitionDir = "input";
		final String outputDir = "output";
		final SSSPJobConfiguration jobConfig = new SSSPJobConfiguration();
		final MasterInputPartitioner inputPartitioner = new RoadNetInputPartitioner(partitionPerWorker);
		final MasterOutputEvaluator<SSSPQueryValues> outputCombiner = new SSSPOutputEvaluator();

		// Start machines
		System.out.println("Starting machines");
		final RunUtils<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> testUtils = new RunUtils<>();
		MasterMachine<SSSPQueryValues> master = testUtils.startSetup(args[0], inputFile,
				inputPartitionDir, inputPartitioner, outputCombiner, outputDir, jobConfig, new RoadNetVertexInputReader());

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

			// Warm up queries
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 4304982, 7031164)); // Very short ST-Echterdingen->ST-HBF
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 4304982, 7031164));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 4304982, 7031164));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 4304982, 7031164));
			//			master.waitForAllQueriesFinish();
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 7031164, 4304982));
			//			master.waitForAllQueriesFinish();

			//			// Test sequence
			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821)); // Medium PF->HB
			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011)); // Short Heidelberg->Heilbronn
			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832)); // Short RT->ST
			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 4982624)); // Short ST-HBF->TU
			master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337)); // Very short Meersburg->Pfullendorf
			//			master.waitForAllQueriesFinish();
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821)); // Medium PF->HB
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011)); // Short Heidelberg->Heilbronn
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832)); // Short RT->ST
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 4982624)); // Short ST-HBF->TU
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337)); // Very short Meersburg->Pfullendorf
			//			master.waitForAllQueriesFinish();
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821)); // Medium PF->HB
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011)); // Short Heidelberg->Heilbronn
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832)); // Short RT->ST
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 4982624)); // Short ST-HBF->TU
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337)); // Very short Meersburg->Pfullendorf
			//			master.waitForAllQueriesFinish();

			// Short RT->ST
			// Ca 7.5s, no vertexmove, without sysout, on PC+local4 and 8s on laptop+local8
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));
			//			master.waitForAllQueriesFinish();
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));
			//			master.waitForAllQueriesFinish();
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));
			//			master.waitForAllQueriesFinish();
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3184057, 7894832));

			// Medium PF->HB
			// Ca 21s, no vertexmove, without sysout, on PC+local4 and 27s on laptop+local8
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 1348329, 3040821));



			// Very short ST-HBF->ST-Airport. Test query "0".
			// Ca 5.5s, no vertexmove, without sysout, on PC+local4 and 6s on laptop+local8
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 7653486));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 7653486));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 7653486));
			//			master.waitForQueryFinish((queryIndex - 1));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 7653486));

			// Very short Meersburg->Pfullendorf. Test query "1"
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337));
			//
			// Short Heidelberg->Heilbronn. Test query "2"
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8272129, 115011));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 115011, 8272129));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 115011, 8272129));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 115011, 8272129));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 115011, 8272129));


			//			Thread.sleep(20000);
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2557651, 7653486));
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 8693095, 2075337));

			//			Thread.sleep(5000);
			//			Thread.sleep(5000);
			//			Thread.sleep(5000);
			//			Thread.sleep(5000);
			//			// Big query through BW, Ludwigshafen->Heilbronn
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 2942985, 6663036));
			//
			//			// Short Mengen->Saulgau
			//			master.startQuery(new SSSPQueryValues(queryIndex++, 3080719, 609074));

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
