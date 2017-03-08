package mthesis.concurrent_graph.plotting;

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

public class JFreeChartPlotter {

	public static void plotStats(String outputFolder) throws Exception {
		String statsFolder = outputFolder + File.separator + "stats";

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
		CsvDataFile queriesCsv = new CsvDataFile(statsFolder + File.separator + "queries.csv");
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


		// Plot single queries
		for (Integer queryId : queries) {
			int queryHash = queriesHashes.get(queryId);
			String queryName = queryId + "_" + queryHash;

			// Plot query times for all workers accumulated
			CsvDataFile timesCsv = new CsvDataFile(statsFolder + File.separator + queryId + "_times_ms.csv");
			plotCsvColumns(statsFolder, "AllStepTimes_" + queryName, "Superstep", "Time (ms)", 1,
					new ColumnToPlot[] {
							new ColumnToPlot(null, timesCsv, 0, 1),
							new ColumnToPlot(null, timesCsv, 1, 1)
					});
			List<ColumnToPlot> timeColumns = new ArrayList<>();
			for (int i = 2; i < timesCsv.Captions.length; i++) {
				timeColumns.add(new ColumnToPlot(null, timesCsv, i, 1));
			}
			plotCsvColumns(statsFolder, "AllWorkerTimes_" + queryName, "Superstep", "Time (ms)", 1, timeColumns);


			// Plot stats per worker
			Map<Integer, CsvDataFile> workerCsvs = new HashMap<>();
			String[] workerCsvCaptions = null;
			for (Integer worker : workers) {
				CsvDataFile csv = new CsvDataFile(statsFolder + File.separator + queryId + "_" + worker + "_all.csv");
				workerCsvs.put(worker, csv);
				workerCsvCaptions = csv.Captions;
			}

			//			plotWorkerStats(statsFolder, "ActiveVertices_" + queryName, "ActiveVertices", workerCsvs, 0, 1);
			//			plotWorkerStats(statsFolder, "WorkerTimes_" + queryName, "WorkerTimes", workerCsvs, 1, 1);
			for (int iCol = 0; iCol < workerCsvCaptions.length; iCol++) {
				plotWorkerStats(statsFolder, "Worker" + workerCsvCaptions[iCol] + "_" + queryName, workerCsvCaptions[iCol], workerCsvs,
						iCol, 1);
			}
		}

		// Plot query compares
		plotQueryComparisons(statsFolder, "all", queries, queriesStats);
		for (Entry<Integer, List<Integer>> hashQueries : queriesByHash.entrySet()) {
			plotQueryComparisons(statsFolder, "hash" + hashQueries.getKey(), hashQueries.getValue(), queriesStats);
		}
	}

	private static void plotWorkerStats(String outputFolder, String name, String axisTitleY,
			Map<Integer, CsvDataFile> workerCsvs, int columnIndex, double factor) throws IOException {
		List<ColumnToPlot> columns = new ArrayList<>(workerCsvs.size());
		for (Entry<Integer, CsvDataFile> wCsv : workerCsvs.entrySet()) {
			columns.add(new ColumnToPlot("Worker " + wCsv.getKey(), wCsv.getValue(), columnIndex, 0));
		}
		plotCsvColumns(outputFolder, name, "Superstep", axisTitleY, factor, columns);
	}

	private static void plotQueryComparisons(String statsFolder, String name, List<Integer> queriesToPlot,
			Map<Integer, Double[]> queriesStats) throws IOException {
		plotQueryTimeComparison(statsFolder, "QueriesDurations_" + name, queriesToPlot, queriesStats, 0);
		plotQueryTimeComparison(statsFolder, "QueriesComputeTimes_" + name, queriesToPlot, queriesStats, 1);
	}

	private static void plotQueryTimeComparison(String statsFolder, String plotName, List<Integer> queriesToPlot,
			Map<Integer, Double[]> queriesTimes, int column) throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		final XYSeries series = new XYSeries("Query times");
		for (int iQ = 0; iQ < queriesToPlot.size(); iQ++) {
			series.add(iQ, queriesTimes.get(queriesToPlot.get(iQ))[column]);
		}
		dataset.addSeries(series);
		plotDataset(statsFolder, plotName, "Query", "Time (ms)", dataset);
	}

	//	private static void plotQueryComparisonSuperstepTimes(String statsFolder, String name, List<Integer> queriesToPlot) throws IOException {
	//
	//	}


	public static void plotCsvColumns(String outputFolder, String name, String axisTitleX, String axisTitleY,
			double factor, List<ColumnToPlot> columns)
			throws IOException {
		plotCsvColumns(outputFolder, name, axisTitleX, axisTitleY, factor, columns.toArray(new ColumnToPlot[0]));
	}

	public static void plotCsvColumns(String outputFolder, String name, String axisTitleX, String axisTitleY,
			double factor, ColumnToPlot[] columns)
			throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		for (ColumnToPlot col : columns) {
			dataset.addSeries(col.Table.getColumnDataset(col.ColumnIndex, factor, col.OptionalName, col.StartRow));
		}
		plotDataset(outputFolder, name, axisTitleX, axisTitleY, dataset);
	}

	//	public static void plotCsvAllColumns(String outputFolder, String name, String axisTitleX, String axisTitleY, CsvDataFile data)
	//			throws IOException {
	//		plotDataset(outputFolder, name, axisTitleX, axisTitleY, data.getTableDatasets());
	//	}


	public static void plotDataset(String outputFolder, String name, String axisTitleX, String axisTitleY, XYDataset dataset)
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
		ChartUtilities.saveChartAsPNG(new File(outputFolder + File.separator + name + ".png"), chart, 1600, 1200, info);
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
			plotStats("output");
			System.out.println("Plot finished");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
