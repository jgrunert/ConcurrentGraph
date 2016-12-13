package mthesis.concurrent_graph.master.input;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and converts input file.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class BaseMasterInputReader {
	protected static final Logger logger = LoggerFactory.getLogger(BaseMasterInputReader.class.getCanonicalName());

	protected final int partitionSize;
	protected final String partitionOutputDir;

	protected BaseMasterInputReader(int partitionSize, String partitionOutputDir) {
		super();
		this.partitionSize = partitionSize;
		this.partitionOutputDir = partitionOutputDir;
	}

	/**
	 * Reads input file and converts it.
	 */
	public abstract void readAndPartition(String inputFile);

	public abstract void close();

	public abstract List<String> getPartitionFiles();
}
