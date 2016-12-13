package mthesis.concurrent_graph;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import mthesis.concurrent_graph.examples.CCDetectVertex;
import mthesis.concurrent_graph.examples.CCDetectVertexValue;
import mthesis.concurrent_graph.examples.CCOutputWriter;
import mthesis.concurrent_graph.examples.EdgeListReader;
import mthesis.concurrent_graph.master.BaseMasterOutputCombiner;
import mthesis.concurrent_graph.master.MasterMachine;
import mthesis.concurrent_graph.master.input.BaseInputPartitionDistributor;
import mthesis.concurrent_graph.master.input.BaseMasterInputReader;
import mthesis.concurrent_graph.master.input.ContinousInputPartitionDistributor;
import mthesis.concurrent_graph.playground.KryoTest;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.worker.WorkerMachine;

public class QuickTest {

	public static void main(String[] args) throws Exception {
		final int numWorkers = 4;
		final String host = "localhost";
		final int basePort = 23499;
		final String inputDir = "input";
		final String outputDir = "output";


		//		final String inputFile = "../../Data/cctest.txt";
		//		final BaseMasterInputReader inputReader = new VertexEdgesInputReader(1, inputDir);
		//		final BaseInputPartitionDistributor inputDistributor = new ContinousInputPartitionDistributor();
		//		final BaseMasterOutputCombiner outputCombiner = new CCOutputWriter();

		final String inputFile = "../../Data/Wiki-Vote.txt";
		final BaseMasterInputReader inputReader = new EdgeListReader(4000, inputDir);
		final BaseInputPartitionDistributor inputDistributor = new ContinousInputPartitionDistributor();
		final BaseMasterOutputCombiner outputCombiner = new CCOutputWriter();

		final Class<? extends AbstractVertex> vertexClass = CCDetectVertex.class;

		final ByteArrayOutputStream stream1 = new ByteArrayOutputStream();
		final Output output1 = new Output(stream1);
		final Kryo kryo = new Kryo();
		kryo.register(CCDetectVertexValue.class);

		final CCDetectVertexValue value1 = new CCDetectVertexValue();
		value1.Value1 = 1024;
		value1.Value2 = 100000;

		kryo.writeObject(output1, value1);
		output1.flush();
		final byte[] buffer = stream1.toByteArray(); // Serialization done, get bytes


		KryoTest.testSerialize();
		KryoTest.testSerializeDeserialize();

		//
		//
		//		//Thread.sleep(10000);
		//
		//
		//		final Map<Integer, Pair<String, Integer>> allCfg = new HashMap<>();
		//		final List<Integer> allWorkerIds= new ArrayList<>();
		//		allCfg.put(-1, new Pair<String, Integer>(host, basePort));
		//		for(int i = 0; i < numWorkers; i++) {
		//			allWorkerIds.add(i);
		//			allCfg.put(i, new Pair<String, Integer>(host, basePort + 1 + i));
		//		}
		//
		//		System.out.println("Starting");
		//		//final MasterNode master =
		//		startMaster(allCfg, -1, allWorkerIds, inputReader, inputFile, inputDistributor, outputCombiner, outputDir);
		//
		//		final List<WorkerMachine> workers = new ArrayList<>();
		//		for(int i = 0; i < numWorkers; i++) {
		//			workers.add(startWorker(allCfg, i, allWorkerIds, outputDir, vertexClass));
		//		}


		//		master.waitUntilStarted();
		//		worker0.waitUntilStarted();
		//		worker1.waitUntilStarted();
		//		System.out.println("All started");
		//		Thread.sleep(240000);

		//		System.out.println("Shutting down");
		//		master.stop();
		//		worker0.stop();
		//		worker1.stop();
		//		System.out.println("End");
	}

	private static WorkerMachine startWorker(Map<Integer, Pair<String, Integer>> allCfg,
			int id, List<Integer> allWorkers, String output,
			Class<? extends AbstractVertex> vertexClass) {
		final WorkerMachine node = new WorkerMachine(allCfg, id, allWorkers, -1, output, vertexClass);
		node.start();
		return node;
	}

	private static MasterMachine startMaster(Map<Integer, Pair<String, Integer>> allMachines, int id, List<Integer> workerIds,
			BaseMasterInputReader inputReader, String inputFile, BaseInputPartitionDistributor inputDistributor,
			BaseMasterOutputCombiner outputCombiner, String outputDir) {
		final MasterMachine node = new MasterMachine(allMachines, id, workerIds, inputReader, inputFile, inputDistributor, outputCombiner, outputDir);
		node.start();
		return node;
	}
}
