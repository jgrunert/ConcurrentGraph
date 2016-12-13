package mthesis.concurrent_graph.playground;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import mthesis.concurrent_graph.examples.CCDetectVertexValue;

public class KryoTest {
	public static void testSerialize() {
		System.out.println("/////// testSerialize");

		final Kryo kryo = new Kryo();
		kryo.register(CCDetectVertexValue.class);

		Random rd;
		final int testRuns = 10000000;
		long startTime;
		long time;
		final CCDetectVertexValue test = new CCDetectVertexValue();
		byte[] serialized;
		int size;

		// Control test
		rd = new Random(0);
		startTime = System.currentTimeMillis();
		for(int i = 0; i < testRuns; i++) {
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("Creaton only in " + time + "ms, " + testRuns / time + " K/s");

		rd = new Random(0);
		startTime = System.currentTimeMillis();
		size = 0;
		for(int i = 0; i < testRuns; i++) {
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
			final ByteBuffer bb = ByteBuffer.allocate(8);
			bb.putInt(test.Value1);
			bb.putInt(test.Value2);
			serialized = bb.array();
			size += 8;
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("ByteBuffer in " + time + "ms, " + testRuns / time + " K/s. Size: " + size / 1024 + "kb");


		rd = new Random(0);
		startTime = System.currentTimeMillis();
		size = 0;
		for(int i = 0; i < testRuns; i++) {
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
			final ByteArrayOutputStream stream = new ByteArrayOutputStream();
			final Output output = new Output(stream);
			kryo.writeObject(output, test);
			output.flush();
			serialized = stream.toByteArray(); // Serialization done, get bytes
			size += serialized.length;
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("Kryo unpooled in " + time + "ms, " + testRuns / time + " K/s. Size: " + size / 1024 + "kb");


		final ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
		final Output output2 = new Output(stream2);
		rd = new Random(0);
		startTime = System.currentTimeMillis();
		size = 0;
		for(int i = 0; i < testRuns; i++) {
			stream2.reset();
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
			kryo.writeObject(output2, test);
			output2.flush();
			serialized = stream2.toByteArray(); // Serialization done, get bytes
			size += serialized.length;
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("Kryo pooled in " + time + "ms, " + testRuns / time + " K/s. Size: " + size / 1024 + "kb");

		rd = new Random(0);
		startTime = System.currentTimeMillis();
		size = 0;
		for(int i = 0; i < testRuns; i++) {
			stream2.reset();
			test.Value1 = rd.nextInt(1000);
			test.Value2 = rd.nextInt(1000);
			kryo.writeObject(output2, test);
			output2.flush();
			serialized = stream2.toByteArray(); // Serialization done, get bytes
			size += serialized.length;
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("Kryo pooled values<1000 in " + time + "ms, " + testRuns / time + " K/s. Size: " + size / 1024 + "kb");
	}



	public static void testSerializeDeserialize() {
		System.out.println("/////// testSerializeDeserialize");
		final Kryo kryo = new Kryo();
		kryo.register(CCDetectVertexValue.class);

		Random rd;
		final int testRuns = 1000000;
		long startTime;
		long time;
		final CCDetectVertexValue test = new CCDetectVertexValue();
		CCDetectVertexValue test2 = new CCDetectVertexValue();
		byte[] serialized;
		int size;

		// Control test
		rd = new Random(0);
		startTime = System.currentTimeMillis();
		for(int i = 0; i < testRuns; i++) {
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
			test2 = new CCDetectVertexValue();
			test2.Value1 = test.Value1;
			test2.Value2 = test.Value2;
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("Creaton only in " + time + "ms, " + testRuns / time + " K/s");

		rd = new Random(0);
		startTime = System.currentTimeMillis();
		size = 0;
		for(int i = 0; i < testRuns; i++) {
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
			final ByteBuffer bb = ByteBuffer.allocate(8);
			bb.putInt(test.Value1);
			bb.putInt(test.Value2);
			serialized = bb.array();
			size += 8;
			final ByteBuffer bb2 = ByteBuffer.wrap(serialized);
			test2 = new CCDetectVertexValue();
			test2.Value1 = bb2.getInt();
			test2.Value2 = bb2.getInt();
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("ByteBuffer in " + time + "ms, " + testRuns / time + " K/s. Size: " + size / 1024 + "kb");


		final ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
		final Output output2 = new Output(stream2);
		rd = new Random(0);
		startTime = System.currentTimeMillis();
		size = 0;
		for(int i = 0; i < testRuns; i++) {
			stream2.reset();
			test.Value1 = rd.nextInt();
			test.Value2 = rd.nextInt();
			kryo.writeObject(output2, test);
			output2.flush();
			serialized = stream2.toByteArray(); // Serialization done, get bytes
			size += serialized.length;

			final ByteArrayInputStream streamIn = new ByteArrayInputStream(serialized);
			final Input input = new Input(streamIn);
			test2 = kryo.readObject(input, CCDetectVertexValue.class);
		}
		time = (System.currentTimeMillis() - startTime);
		System.out.println("Kryo pooled in " + time + "ms, " + testRuns / time + " K/s. Size: " + size / 1024 + "kb");
	}
}
