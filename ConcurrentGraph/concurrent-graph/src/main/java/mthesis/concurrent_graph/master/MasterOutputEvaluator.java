package mthesis.concurrent_graph.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQuery;

/**
 * Reads output file partitions. Can be used to combine or evaluate any output.
 * 
 * @author Jonas Grunert
 *
 */
public abstract class MasterOutputEvaluator<G extends BaseQuery> {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Reads input file
	 */
	public abstract void evaluateOutput(String outputDir, G query);
}
