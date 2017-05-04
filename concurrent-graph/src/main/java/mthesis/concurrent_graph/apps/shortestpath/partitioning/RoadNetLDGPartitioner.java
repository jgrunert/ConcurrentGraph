package mthesis.concurrent_graph.apps.shortestpath.partitioning;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.util.FileUtil;

/**
 * Partitioner for given road network graph
 *
 * @author Jonas Grunert
 *
 */
public class RoadNetLDGPartitioner extends MasterInputPartitioner {

	private final int partitionsPerWorker;

	private LinearDeterministicGreedy LDG;
	
	public RoadNetLDGPartitioner(int partitionsPerWorker) {
		this.partitionsPerWorker = partitionsPerWorker;
	}

	@Override
	public Map<Integer, List<String>> partition(String inputFile, String outputDir, List<Integer> workers) throws IOException {

		//////////// Determine partition count and create partitions ////////////
		// Get number of vertices
		int numVertices = 0;
		int numEdgesAll = 0;
		try (DataInputStream reader = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)))) {
			numVertices = reader.readInt();
		}

		// Create partition files
		FileUtil.createDirOrEmptyFiles(outputDir);
		int numPartitions = workers.size() * partitionsPerWorker;
		Map<Integer, List<String>> partitionsAssignements = new HashMap<>();
		List<DataOutputStream> partitionFiles = new ArrayList<>(numPartitions);
		int iPTmp = 0;
		for (int iW = 0; iW < workers.size(); iW++) {
			List<String> workerPartitions = new ArrayList<>(partitionsPerWorker);
			partitionsAssignements.put(workers.get(iW), workerPartitions);
			for (int iWP = 0; iWP < (numPartitions / workers.size()); iWP++) {
				String partitionFileName = outputDir + File.separator + iPTmp + ".bin";
				workerPartitions.add(partitionFileName);
				DataOutputStream partitionFileWriter = new DataOutputStream(
						new BufferedOutputStream(new FileOutputStream(partitionFileName)));
				logger.trace("Write partition file " + new File(partitionFileName).getAbsolutePath());
				partitionFiles.add(partitionFileWriter);
				iPTmp++;
			}
		}


		//////////// Parse and assign partition vertices ////////////
		// Forked from Hashed partitioning
		// TODO LDG partitioning
		
		LDG = new LinearDeterministicGreedy(numPartitions,numVertices);

		int[] partitionVertexCount = new int[numPartitions];
		Int2IntMap vertexPartitions = new Int2IntOpenHashMap(numVertices);
		try (DataInputStream reader = new DataInputStream(
				new BufferedInputStream(new FileInputStream(inputFile)))) {
			reader.readInt();
			for (int iV = 0; iV < numVertices; iV++) {
				int vertexId = reader.readInt();
				reader.readDouble(); // Ignore latitude
				reader.readDouble(); // Ignore longitude
				int numEdges = reader.readInt();
				
				Set<Integer> neighbors = new HashSet<Integer>();
				for (int iEdge = 0; iEdge < numEdges; iEdge++) {
					Integer neighbor = reader.readInt();
					reader.readDouble();
					neighbors.add(neighbor);
				}

				int partitionIndex = LDG.getPartitionID(vertexId, neighbors);
				vertexPartitions.put(vertexId, partitionIndex);
				partitionVertexCount[partitionIndex]++;
			}
		}

		//////////// Write out partitions ////////////
		// Write partition size
		for (int iP = 0; iP < numPartitions; iP++) {
			partitionFiles.get(iP).writeInt(partitionVertexCount[iP]);
		}

		// Write vertices
		try (DataInputStream reader = new DataInputStream(
				new BufferedInputStream(new FileInputStream(inputFile)))) {
			reader.readInt();
			for (int iV = 0; iV < numVertices; iV++) {
				int vertexId = reader.readInt();
				reader.readDouble();
				reader.readDouble();
				int numEdges = reader.readInt();

				int partitionIndex = vertexPartitions.get(vertexId);
				DataOutputStream partitionFileWriter = partitionFiles.get(partitionIndex);

				partitionFileWriter.writeInt(vertexId);
				partitionFileWriter.writeInt(numEdges);
				for (int iEdge = 0; iEdge < numEdges; iEdge++) {
					partitionFileWriter.writeInt(reader.readInt());
					partitionFileWriter.writeDouble(reader.readDouble());
				}

				numEdgesAll += numEdges;
			}
		}


		//////////// Close partition files ////////////
		for (DataOutputStream partitionFile : partitionFiles) {
			partitionFile.close();
		}

		logger.info("Partitioned " + numVertices + " vertices and " + numEdgesAll + " edges into " + numPartitions
				+ " partitions");

		return partitionsAssignements;
	}

}
