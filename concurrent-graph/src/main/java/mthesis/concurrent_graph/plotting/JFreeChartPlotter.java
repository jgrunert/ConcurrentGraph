package mthesis.concurrent_graph.plotting;

import java.awt.BasicStroke;
import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.Configuration;
import mthesis.concurrent_graph.util.FileUtil;

public class JFreeChartPlotter {

	private static final Logger logger = LoggerFactory.getLogger(JFreeChartPlotter.class);

	public static void plotStats(String outputFolder, int samplingFactor) throws Exception {
		logger.info("Start plotting");

		String statsFolder = outputFolder + File.separator + "stats";
		String plotsOutDir = statsFolder + File.separator + "plots" + samplingFactor;
		FileUtil.createDirOrEmptyFiles(plotsOutDir);

		List<Integer> workers = new ArrayList<>();
		List<Integer> queries = new ArrayList<>();
		Map<Integer, Double[]> queriesStats = new HashMap<>();
		Map<Integer, Integer> queriesHashes = new HashMap<>();
		Map<Integer, List<Integer>> queriesByHash = new HashMap<>();


		// Load setup file
		BufferedReader setupReader = new BufferedReader(new FileReader(outputFolder + File.separator + "setup.txt"));
		int masterId = Integer.parseInt(setupReader.readLine().substring(10));
		setupReader.readLine();
		String line;
		while ((line = setupReader.readLine()) != null) {
			int idTmp = Integer.parseInt(line.split("\t")[1]);
			if (idTmp != masterId)
				workers.add(idTmp);
		}
		setupReader.close();


		// Load queries file
		CsvDataFile queriesCsv = new CsvDataFile(statsFolder + File.separator + "queries.csv", samplingFactor);
		for (int i = 0; i < queriesCsv.NumDataRows; i++) {
			int queryId = (int) queriesCsv.Data[i][0];
			int queryHash = (int) queriesCsv.Data[i][1];
			queries.add(queryId);
			queriesStats.put(queryId, new Double[] { queriesCsv.Data[i][2], queriesCsv.Data[i][3] });
			queriesHashes.put(queryId, queryHash);

			List<Integer> hashQs = queriesByHash.get(queryHash);
			if (hashQs == null) {
				hashQs = new ArrayList<>();
				queriesByHash.put(queryHash, hashQs);
			}
			hashQs.add(queryId);
		}


		// Plot worker stats
		if (Configuration.getPropertyBoolDefault("PlotWorkerStats", false)) {
			// Plot all worker stats
			Map<Integer, CsvDataFile> workerStatsCsvs = new HashMap<>();
			String[] workerStatsCaptions = null;
			for (Integer workerId : workers) {
				CsvDataFile csv = new CsvDataFile(statsFolder + File.separator + "worker" + workerId + "_all.csv", samplingFactor);
				workerStatsCsvs.put(workerId, csv);
				workerStatsCaptions = csv.Captions;
			}
			if (workerStatsCaptions != null) {
				for (int iStat = 1; iStat < workerStatsCaptions.length; iStat++) {
					plotWorkerStats(statsFolder, "WorkerStats_" + workerStatsCaptions[iStat], workerStatsCaptions[iStat], workerStatsCsvs,
							iStat, 1,
							plotsOutDir);
				}
			}

			// Plot worker average stats
			CsvDataFile workerAvgCsv = new CsvDataFile(statsFolder + File.separator + "workerAvgStats.csv", samplingFactor);
			for (int iStat = 1; iStat < workerAvgCsv.Captions.length; iStat++) {
				final XYSeriesCollection dataset = new XYSeriesCollection();
				final XYSeries series = new XYSeries(workerAvgCsv.Captions[iStat]);
				for (int i = 0; i < workerAvgCsv.NumDataRows; i++) {
					series.add(workerAvgCsv.Data[i][0], workerAvgCsv.Data[i][iStat]);
				}
				dataset.addSeries(series);
				plotDataset(outputFolder, "WorkerAvgStats_" + workerAvgCsv.Captions[iStat], "Time (s)", workerAvgCsv.Captions[iStat],
						dataset, plotsOutDir);
			}

			for (Integer workerId : workers) {
				// Plot query times for all workers accumulated
				CsvDataFile timesCsv = new CsvDataFile(statsFolder + File.separator + "worker" + workerId + "_times_ms.csv",
						samplingFactor);
				List<ColumnToPlot> timeColumns = new ArrayList<>();
				for (int i = 0; i < timesCsv.Captions.length; i++) {
					timeColumns.add(new ColumnToPlot(null, timesCsv, i, 1));
				}
				plotCsvColumns(statsFolder, "WorkerTimes_" + workerId, "Sample", "Time (ms)", 1, timeColumns, plotsOutDir);
			}

			for (Integer workerId : workers) {
				// Plot query times for all workers accumulated, normalized
				CsvDataFile timesCsv = new CsvDataFile(statsFolder + File.separator + "worker" + workerId + "_times_normed_ms.csv",
						samplingFactor);
				List<ColumnToPlot> timeColumns = new ArrayList<>();
				for (int i = 0; i < timesCsv.Captions.length; i++) {
					timeColumns.add(new ColumnToPlot(null, timesCsv, i, 1));
				}
				plotCsvColumns(statsFolder, "WorkerTimes_normed_" + workerId, "Sample", "Time (ms)", 1, timeColumns, plotsOutDir);
			}

			// Plot query times
			List<ColumnToPlot> timeColumns = new ArrayList<>();
			timeColumns.add(new ColumnToPlot(null, queriesCsv, 4, 1));
			timeColumns.add(new ColumnToPlot(null, queriesCsv, 5, 1));
			timeColumns.add(new ColumnToPlot(null, queriesCsv, 6, 1));
			plotCsvColumns(statsFolder, "QueriesTimes", "Query", "Time", 1, timeColumns, plotsOutDir);

			logger.info("Finished plotting worker stats");
		}


		// Plot single queries
		if (Configuration.getPropertyBoolDefault("PlotQueryStats", false)) {
			for (Integer queryId : queries) {
				int queryHash = queriesHashes.get(queryId);
				String queryName = queryId + "_" + queryHash;

				// Plot query times for all workers accumulated
				CsvDataFile timesCsv = new CsvDataFile(statsFolder + File.separator + "query" + queryId + "_times_ms.csv", samplingFactor);
				plotCsvColumns(statsFolder, "Query_AllStepTimes_" + queryName, "Superstep", "Time (ms)", 1,
						new ColumnToPlot[] {
								new ColumnToPlot(null, timesCsv, 0, 1),
								new ColumnToPlot(null, timesCsv, 1, 1)
						}, plotsOutDir);
				List<ColumnToPlot> timeColumns = new ArrayList<>();
				for (int i = 2; i < timesCsv.Captions.length; i++) {
					timeColumns.add(new ColumnToPlot(null, timesCsv, i, 1));
				}
				plotCsvColumns(statsFolder, "Query_AllWorkerTimes_" + queryName, "Superstep", "Time (ms)", 1, timeColumns, plotsOutDir);


				// Plot stats per worker
				Map<Integer, CsvDataFile> workerCsvs = new HashMap<>();
				String[] workerCsvCaptions = null;
				for (Integer worker : workers) {
					CsvDataFile csv = new CsvDataFile(
							statsFolder + File.separator + "query" + queryId + "_" + "worker" + worker + "_all.csv", samplingFactor);
					workerCsvs.put(worker, csv);
					workerCsvCaptions = csv.Captions;
				}

				//			plotWorkerStats(statsFolder, "ActiveVertices_" + queryName, "ActiveVertices", workerCsvs, 0, 1);
				//			plotWorkerStats(statsFolder, "WorkerTimes_" + queryName, "WorkerTimes", workerCsvs, 1, 1);
				for (int iCol = 0; iCol < workerCsvCaptions.length; iCol++) {
					plotWorkerQueryStats(statsFolder, "QueryWorker_" + workerCsvCaptions[iCol] + "_" + queryName, workerCsvCaptions[iCol],
							workerCsvs, iCol, 1, plotsOutDir);
				}
			}
			logger.info("Finished plotting single queries");


			// Plot query compares
			plotQueryComparisons(statsFolder, "all", queries, queriesStats, samplingFactor, plotsOutDir);
			for (Entry<Integer, List<Integer>> hashQueries : queriesByHash.entrySet()) {
				plotQueryComparisons(statsFolder, "" + hashQueries.getKey(), hashQueries.getValue(), queriesStats, samplingFactor,
						plotsOutDir);
			}
			logger.info("Finished plotting query comparisons");
		}
	}


	private static void plotWorkerStats(String outputFolder, String plotName, String axisTitleY,
			Map<Integer, CsvDataFile> workerStatsCsvs, int columnIndex, double factor, String plotsOutDir) throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		for (Entry<Integer, CsvDataFile> worker : workerStatsCsvs.entrySet()) {
			CsvDataFile workerCsv = worker.getValue();
			final XYSeries series = new XYSeries("Worker " + worker.getKey());
			for (int i = 0; i < workerCsv.NumDataRows; i++) {
				series.add(workerCsv.Data[i][0], workerCsv.Data[i][columnIndex]);
			}
			dataset.addSeries(series);
		}
		plotDataset(outputFolder, plotName, "Time (s)", axisTitleY, dataset, plotsOutDir);
	}


	private static void plotWorkerQueryStats(String outputFolder, String name, String axisTitleY,
			Map<Integer, CsvDataFile> workerCsvs, int columnIndex, double factor, String plotsOutDir) throws IOException {
		List<ColumnToPlot> columns = new ArrayList<>(workerCsvs.size());
		for (Entry<Integer, CsvDataFile> wCsv : workerCsvs.entrySet()) {
			columns.add(new ColumnToPlot("Worker " + wCsv.getKey(), wCsv.getValue(), columnIndex, 0));
		}
		plotCsvColumns(outputFolder, name, "Superstep", axisTitleY, factor, columns, plotsOutDir);
	}

	private static void plotQueryComparisons(String statsFolder, String name, List<Integer> queriesToPlot,
			Map<Integer, Double[]> queriesStats, int samplingFactor, String plotsOutDir) throws IOException {
		plotQueryTimeComparison(statsFolder, "QueriesDurations_" + name, queriesToPlot, queriesStats, 0, plotsOutDir);
		plotQueryTimeComparison(statsFolder, "QueriesWorkerTimes_" + name, queriesToPlot, queriesStats, 1, plotsOutDir);
		plotQueryComparisonSuperstepTimes(statsFolder, "QueriesStepDurations_" + name, queriesToPlot, 1, samplingFactor, plotsOutDir);
		plotQueryComparisonSuperstepTimes(statsFolder, "QueriesStepWorkerTimes_" + name, queriesToPlot, 2, samplingFactor, plotsOutDir);
	}

	private static void plotQueryTimeComparison(String statsFolder, String plotName, List<Integer> queriesToPlot,
			Map<Integer, Double[]> queriesTimes, int column, String plotsOutDir) throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		final XYSeries series = new XYSeries("Query times");
		for (int iQ = 0; iQ < queriesToPlot.size(); iQ++) {
			series.add(iQ, queriesTimes.get(queriesToPlot.get(iQ))[column]);
		}
		dataset.addSeries(series);
		plotDataset(statsFolder, plotName, "Query", "Time (ms)", dataset, plotsOutDir);
	}

	private static void plotQueryComparisonSuperstepTimes(String statsFolder, String plotName, List<Integer> queriesToPlot,
			int columnIndex, int samplingFactor, String plotsOutDir)
			throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		for (Integer queryId : queriesToPlot) {
			CsvDataFile timesCsv = new CsvDataFile(statsFolder + File.separator + "query" + queryId + "_times_ms.csv", samplingFactor);
			dataset.addSeries(timesCsv.getColumnDataset(0, 1, "Query " + queryId, columnIndex));
		}
		plotDataset(statsFolder, plotName, "Superstep", "Time (ms)", dataset, plotsOutDir);
	}


	public static void plotCsvColumns(String outputFolder, String name, String axisTitleX, String axisTitleY,
			double factor, List<ColumnToPlot> columns, String plotsOutDir)
			throws IOException {
		plotCsvColumns(outputFolder, name, axisTitleX, axisTitleY, factor, columns.toArray(new ColumnToPlot[0]), plotsOutDir);
	}

	public static void plotCsvColumns(String outputFolder, String name, String axisTitleX, String axisTitleY,
			double factor, ColumnToPlot[] columns, String plotsOutDir)
			throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		for (ColumnToPlot col : columns) {
			dataset.addSeries(col.Table.getColumnDataset(col.ColumnIndex, factor, col.OptionalName, col.StartRow));
		}
		plotDataset(outputFolder, name, axisTitleX, axisTitleY, dataset, plotsOutDir);
	}

	//	public static void plotCsvAllColumns(String outputFolder, String name, String axisTitleX, String axisTitleY, CsvDataFile data)
	//			throws IOException {
	//		plotDataset(outputFolder, name, axisTitleX, axisTitleY, data.getTableDatasets());
	//	}


	public static void plotDataset(String outputFolder, String name, String axisTitleX, String axisTitleY, XYDataset dataset,
			String plotsOutDir)
			throws IOException {
		final JFreeChart chart = ChartFactory.createXYLineChart(
				name, // chart title
				axisTitleX, // x axis label
				axisTitleY, // y axis label
				dataset, // data
				PlotOrientation.VERTICAL,
				true, // include legend
				false, // tooltips
				false // urls
		);

		// NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
		chart.setBackgroundPaint(Color.white);

		//	        final StandardLegend legend = (StandardLegend) chart.getLegend();
		//      legend.setDisplaySeriesShapes(true);

		// get a reference to the plot for further customisation...
		final XYPlot plot = chart.getXYPlot();
		plot.setBackgroundPaint(Color.lightGray);
		//    plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.white);

		final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
		for (int i = 0; i < dataset.getSeriesCount(); i++)
			renderer.setSeriesStroke(i, new BasicStroke(1.5f));
		//		renderer.setSeriesLinesVisible(0, false);
		//		renderer.setSeriesShapesVisible(1, false);
		renderer.setBaseShapesVisible(false);
		//		renderer.setBaseShapesFilled(false);
		plot.setRenderer(renderer);

		// change the auto tick unit selection to integer units only...
		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		// OPTIONAL CUSTOMISATION COMPLETED.

		ChartRenderingInfo info = new ChartRenderingInfo(new StandardEntityCollection());
		ChartUtilities.saveChartAsPNG(new File(plotsOutDir + File.separator + name + ".png"), chart, 1200, 900,
				info);
	}


	private static class ColumnToPlot {

		public final String OptionalName;
		public final CsvDataFile Table;
		public final int ColumnIndex;
		public final int StartRow;


		public ColumnToPlot(String optionalName, CsvDataFile table, int columnIndex, int startRow) {
			super();
			OptionalName = optionalName;
			Table = table;
			ColumnIndex = columnIndex;
			StartRow = startRow;
		}
	}



	public static void main(String[] args) {
		try {
			// By default only worker stats
			Configuration.Properties.put("PlotWorkerStats", "true");
			//Configuration.Properties.put("PlotQueryStats", "true");
			String outputDir = args[0];
			//			plotStats(outputDir, 1);
			//			plotStats(outputDir, 4);
			plotStats(outputDir, 8);
			plotStats(outputDir, 16);
			plotStats(outputDir, 32);
			System.out.println("Plot finished");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
