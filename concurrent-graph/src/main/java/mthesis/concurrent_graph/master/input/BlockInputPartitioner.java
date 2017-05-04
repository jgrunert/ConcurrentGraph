package mthesis.concurrent_graph.master.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.util.FileUtil;


/**
 * Partitioner partitioning data into continous blocks
 * 
 * @author Jonas Grunert
 *
 */
public abstract class BlockInputPartitioner extends MasterInputPartitioner {
	private final int partitionSize;

	public BlockInputPartitioner(int partitionMaxVertices) {
		super();
		this.partitionSize = partitionMaxVertices;
	}

	@Override
	public Map<Integer, List<String>> partition(String inputFile, String outputDir, List<Integer> workers) {

		FileUtil.createDirOrEmptyFiles(outputDir);
		final List<String> partitionFiles = new ArrayList<>();
		PrintWriter partitionWriter = null;
		int partitionCounter = 0;
		int partitionSizeCounter = 0;

		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while((line = br.readLine()) != null) {
				if(partitionWriter == null || partitionSizeCounter >= partitionSize) {
					if(partitionWriter != null)
						partitionWriter.close();
					final String fileName = outputDir + File.separator + partitionCounter + ".txt";
					partitionWriter = new PrintWriter(new FileWriter(fileName));
					partitionFiles.add(fileName);
					partitionCounter++;
					partitionSizeCounter = 0;
				}

				partitionWriter.println(line);
				partitionSizeCounter++;
			}
		} catch (final Exception e) {
			logger.error("Failed partition", e);
		}

		if(partitionWriter != null)
			partitionWriter.close();

		return distributePartitions(partitionFiles, workers);
	}

	public abstract Map<Integer, List<String>> distributePartitions(List<String> partitions, List<Integer> workers);
}
