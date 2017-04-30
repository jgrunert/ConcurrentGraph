package mthesis.concurrent_graph.apps.shortestpath.partitioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.master.input.MasterInputPartitioner;

public class PartitioningStrategySelector {

	protected static final Logger logger = LoggerFactory.getLogger(PartitioningStrategySelector.class);

	public static MasterInputPartitioner getPartitioner() {
		int partitionsPerWorker = Configuration.getPropertyInt("PartitionsPerWorker");
		String partitioningMode = Configuration.getPropertyString("InputPartitioner").toLowerCase();

		switch (partitioningMode) {
			case "default":
				logger.info("Partitioning with RoadNetDefaultPartitioner");
				return new RoadNetDefaultPartitioner(partitionsPerWorker);
			case "hashed":
				logger.info("Partitioning with RoadNetHashedPartitioner");
				return new RoadNetHashedPartitioner(partitionsPerWorker);
			case "ldg":
				logger.info("Partitioning with RoadNetLDGPartitioner");
				return new RoadNetLDGPartitioner(partitionsPerWorker);
			case "hotspot":
				logger.info("Partitioning with RoadNetHotspotPartitioner");
				return new RoadNetHotspotPartitioner(partitionsPerWorker);
			default:
				throw new RuntimeException("Unsupported partitioning mode: " + partitioningMode);
		}
	}
}
