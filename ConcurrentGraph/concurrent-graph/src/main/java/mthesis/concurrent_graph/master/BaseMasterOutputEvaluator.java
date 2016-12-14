package mthesis.concurrent_graph.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads output file partitions. Can be used to combine or evaluate any output.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class BaseMasterOutputEvaluator {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Reads input file
	 */
	public abstract void evaluateOutput(String outputDir);
}
