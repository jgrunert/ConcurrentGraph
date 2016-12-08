package mthesis.concurrent_graph.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads output file partitions and writes compbined output.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class AbstractMasterOutputWriter {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractMasterOutputWriter.class.getCanonicalName());

	/**
	 * Reads input file
	 */
	public abstract void writeOutput(String outputDir);
}
