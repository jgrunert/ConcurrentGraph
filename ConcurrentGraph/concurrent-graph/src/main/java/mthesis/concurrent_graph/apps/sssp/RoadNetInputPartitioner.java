package mthesis.concurrent_graph.apps.sssp;

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
import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.util.FileUtil;

/**
 * Partitioner for given road network graph
 *
 * @author Jonas Grunert
 *
 */
public class RoadNetInputPartitioner extends MasterInputPartitioner {

	private final int partitionsPerWorker;

	public RoadNetInputPartitioner(int partitionsPerWorker) {
		this.partitionsPerWorker = partitionsPerWorker;
	}

	@Override
	public Map<Integer, List<String>> partition(String inputFile, String outputDir, List<Integer> workers) throws IOException {
		// Get number of vertices
		int numVertices = 0;
		int numEdgesAll = 0;
		try (DataInputStream reader = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)))) {
			numVertices = reader.readInt();
		}

		FileUtil.makeCleanDirectory(outputDir);

		// Determine and assign partitions
		int numPartitions = workers.size() * partitionsPerWorker;
		int vertsPerPartition = (numVertices + numPartitions - 1) / numPartitions;
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


		// Parse and partition vertices
		if (Configuration.getPropertyBoolDefault("HashedPartitioning", false)) {
			// Hashed partitioning

			// Assign vertices
			int[] partitionVertexCount = new int[numPartitions];
			Int2IntMap vertexPartitions = new Int2IntOpenHashMap(numVertices);
			try (DataInputStream reader = new DataInputStream(
					new BufferedInputStream(new FileInputStream(inputFile)))) {
				reader.readInt();
				for (int iV = 0; iV < numVertices; iV++) {
					int vertexId = reader.readInt();
					reader.readDouble();
					reader.readDouble();
					int numEdges = reader.readInt();
					for (int iEdge = 0; iEdge < numEdges; iEdge++) {
						reader.readInt();
						reader.readDouble();
					}

					int partitionIndex = (vertexId * 31 + numEdges) % numPartitions;
					vertexPartitions.put(vertexId, partitionIndex);
					partitionVertexCount[partitionIndex]++;
				}
			}

			// Write out partitions
			for (int iP = 0; iP < numPartitions; iP++) {
				partitionFiles.get(iP).writeInt(partitionVertexCount[iP]);
			}
			try (DataInputStream reader = new DataInputStream(
					new BufferedInputStream(new FileInputStream(inputFile)))) {
				reader.readInt();
				for (int iV = 0; iV < numVertices; iV++) {
					int vertexId = reader.readInt();
					reader.readDouble();
					reader.readDouble();
					int numEdges = reader.readInt();

					int partitionIndex = (vertexId * 31 + numEdges) % numPartitions;
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
		}
		else {
			try (DataInputStream reader = new DataInputStream(
					new BufferedInputStream(new FileInputStream(inputFile)))) {
				// Original OSM data partitioning
				int iNode = 0;
				reader.readInt();

				for (int iP = 0; iP < numPartitions; iP++) {
					int partitionVerts = Math.min(vertsPerPartition, numVertices - iNode);
					DataOutputStream partitionFileWriter = partitionFiles.get(iP);
					//System.out.println(iP + " has " + partitionVerts);

					partitionFileWriter.writeInt(partitionVerts);
					for (int iV = 0; iV < partitionVerts; iV++) {
						partitionFileWriter.writeInt(reader.readInt());
						// We don't need latitude and longitude
						reader.readDouble();
						reader.readDouble();

						int numEdges = reader.readInt();
						partitionFileWriter.writeInt(numEdges);
						for (int iEdge = 0; iEdge < numEdges; iEdge++) {
							partitionFileWriter.writeInt(reader.readInt());
							partitionFileWriter.writeDouble(reader.readDouble());
						}

						numEdgesAll += numEdges;
						iNode++;
					}
				}
			}
		}

		// Close partition files
		for (DataOutputStream partitionFile : partitionFiles) {
			partitionFile.close();
		}

		logger.info("Partitioned " + numVertices + " vertices and " + numEdgesAll + " edges into " + numPartitions
				+ " partitions");

		return partitionsAssignements;
	}

}
