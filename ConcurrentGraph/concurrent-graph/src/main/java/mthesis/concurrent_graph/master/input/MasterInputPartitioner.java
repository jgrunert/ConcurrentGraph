package mthesis.concurrent_graph.master.input;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for a partitioner which partitions given input data into multiple partitions.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class MasterInputPartitioner {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public abstract Map<Integer, List<String>> partition(String inputFile, String outputDir, List<Integer> workers);
}
