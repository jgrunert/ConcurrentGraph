package mthesis.concurrent_graph.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads output file partitions and writes compbined output.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class BaseMasterOutputCombiner {
	protected static final Logger logger = LoggerFactory.getLogger(BaseMasterOutputCombiner.class.getCanonicalName());

	/**
	 * Reads input file
	 */
	public abstract void evaluateOutput(String outputDir);
}
