package mthesis.concurrent_graph.apps.shortestpath.partitioning;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;
import mthesis.concurrent_graph.util.FileUtil;
import mthesis.concurrent_graph.util.Pair;

/**
 * Hotspot partitioner for given road network graph.
 * Vertices dedicated to the closest hotspot.
 *
 * @author Jonas Grunert
 *
 */
public class RoadNetHotspotPartitioner extends MasterInputPartitioner {

	private final int partitionsPerWorker;

	public RoadNetHotspotPartitioner(int partitionsPerWorker) {
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

		int numSpots = Configuration.getPropertyIntDefault("HotspotPartitionerSpots", 16);
		double rangeDivisor = Configuration.getPropertyIntDefault("HotspotPartitionerRange", 50000);
		// Load cities and their sizes
		List<String> spots = new ArrayList<>();
		List<Double> spotsRanges = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(
				new FileReader(inputFile.split(".bin")[0] + "_hotspots_sizes.csv"))) {
			String line;
			while ((line = reader.readLine()) != null && spots.size() < numSpots) {
				String[] split = line.split(";");
				spots.add(split[0].toLowerCase());
				spotsRanges.add((double) Integer.parseInt(split[1]) / rangeDivisor);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		// Load cities coordinates
		Map<String, Pair<Double, Double>> spotsCoordinates = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(
				new FileReader(inputFile.split(".bin")[0] + "_hotspots_coords.csv"))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(";");
				spotsCoordinates.put(split[0].toLowerCase(),
						new Pair<>(Double.parseDouble(split[1]), Double.parseDouble(split[2])));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		List<Pair<Double, Double>> spotCoordinates = new ArrayList<>(spots.size());
		for (int i = 0; i < spots.size(); i++) {
			spotCoordinates.add(spotsCoordinates.get(spots.get(i)));
		}

		FileUtil.createDirOrEmptyFiles(outputDir);

		// Determine and assign partitions
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


		// Parse and partition vertices
		// Hashed partitioning

		// Assign vertices
		int[] sportVertexCount = new int[numSpots];
		Int2IntMap vertexSpots = new Int2IntOpenHashMap(numVertices);
		try (DataInputStream reader = new DataInputStream(
				new BufferedInputStream(new FileInputStream(inputFile)))) {
			reader.readInt();
			for (int iV = 0; iV < numVertices; iV++) {
				int vertexId = reader.readInt();
				double lat = reader.readDouble();
				double lon = reader.readDouble();
				int numEdges = reader.readInt();
				for (int iEdge = 0; iEdge < numEdges; iEdge++) {
					reader.readInt();
					reader.readDouble();
				}

				int partitionIndex = -1;
				double smallestPartitionDist = Double.MAX_VALUE;
				// Try to find closest spot where vertex within range
				for (int i = 0; i < spotCoordinates.size(); i++) {
					Pair<Double, Double> spotCoords = spotCoordinates.get(i);
					double spotRange = spotsRanges.get(i);
					double dist = calcVector2Dist(lat, lon, spotCoords.first, spotCoords.second);
					if (dist <= spotRange && dist < smallestPartitionDist) {
						smallestPartitionDist = dist;
						partitionIndex = i;
					}
				}
				// Try to find any closest spot if no spot in range
				if (partitionIndex == -1) {
					for (int i = 0; i < spotCoordinates.size(); i++) {
						Pair<Double, Double> spotCoords = spotCoordinates.get(i);
						double dist = calcVector2Dist(lat, lon, spotCoords.first, spotCoords.second);
						if (dist < smallestPartitionDist) {
							smallestPartitionDist = dist;
							partitionIndex = i;
						}
					}
				}

				vertexSpots.put(vertexId, partitionIndex);
				sportVertexCount[partitionIndex]++;
			}
		}


		// Assign spots to partitions
		Map<Integer, Integer> spotPartitions = new HashMap<>(numPartitions);
		Map<Integer, Integer> partitionSizes = new HashMap<>(numPartitions);
		for (int iP = 0; iP < numPartitions; iP++) {
			partitionSizes.put(iP, 0);
		}
		for (int iS = 0; iS < numSpots; iS++) {
			int bestPartition = 0;
			int bestPartitionSize = Integer.MAX_VALUE;
			for (Entry<Integer, Integer> partition : partitionSizes.entrySet()) {
				if (partition.getValue() < bestPartitionSize) {
					bestPartition = partition.getKey();
					bestPartitionSize = partition.getValue();
				}
			}
			spotPartitions.put(iS, bestPartition);
			partitionSizes.put(bestPartition, partitionSizes.get(bestPartition) + sportVertexCount[iS]);
		}

		int[] partitionVertexCount = new int[numPartitions];
		Int2IntMap vertexPartitions = new Int2IntOpenHashMap(numVertices);
		for (Entry<Integer, Integer> vertex : vertexSpots.entrySet()) {
			int vertexId = vertex.getKey();
			int vertexSpot = vertex.getValue();
			int vertexPartition = spotPartitions.get(vertexSpot);
			vertexPartitions.put(vertexId, vertexPartition);
			partitionVertexCount[vertexPartition]++;
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


		// Close partition files
		for (DataOutputStream partitionFile : partitionFiles) {
			partitionFile.close();
		}

		logger.info("Partitioned " + numVertices + " vertices and " + numEdgesAll + " edges into " + numPartitions
				+ " partitions");

		return partitionsAssignements;
	}

	private static double calcVector2Dist(double aLat, double aLon, double bLat, double bLon) {
		double d0 = aLat - bLat;
		double d1 = aLon - bLon;
		return d0 * d0 + d1 * d1;
	}
}
