package mthesis.concurrent_graph.master;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads input file and writes partitioned input.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class AbstractMasterInputReader {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractMasterInputReader.class);

	/**
	 * Reads input file and partitions it
	 */
	public abstract List<String> readAndPartition(String inputData, String inputDir, int numPartitions);
}
