package mthesis.concurrent_graph.examples;

import java.util.List;

import mthesis.concurrent_graph.master.AbstractMasterInputReader;

/**
 * Reads a file with an ordered list of edges. Format:
 * [vertex0]\t[edge0]
 * [vertex0]\t[edge1]
 * [vertex1]\t[edge0]
 * ...
 * 
 * @author Jonas Grunert
 *
 */
public class EdgeListReader extends AbstractMasterInputReader {
	@Override
	public List<String> readAndPartition(String inputData, String inputDir, int numPartitions) {
		// TODO Auto-generated method stub
		return null;
	}

}
