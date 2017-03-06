package mthesis.concurrent_graph.plotting;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.List;

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
import org.jfree.data.xy.XYSeriesCollection;

public class JFreeChartPlotter {

	public static void plotStats(String statsFolder) throws Exception {
		int queryId = 0;

		CsvDataFile times = new CsvDataFile(statsFolder + File.separator + queryId + "_times_ms.csv");
		CsvDataFile all0 = new CsvDataFile(statsFolder + File.separator + queryId + "_0_all.csv");
		CsvDataFile all1 = new CsvDataFile(statsFolder + File.separator + queryId + "_1_all.csv");
		CsvDataFile all2 = new CsvDataFile(statsFolder + File.separator + queryId + "_2_all.csv");
		CsvDataFile all3 = new CsvDataFile(statsFolder + File.separator + queryId + "_3_all.csv");

		plotCsvAllColumns(statsFolder, queryId + "_SuperstepTimes", "Superstep", "Time (ms)", times);

		plotCsvColumns(statsFolder, queryId + "_ActiveVerts", "Superstep", "ActiveVertices",
				new ColumnToPlot[] {
						new ColumnToPlot("Machine 0", all0, 0),
						new ColumnToPlot("Machine 1", all1, 0),
						new ColumnToPlot("Machine 2", all2, 0),
						new ColumnToPlot("Machine 3", all3, 0)
				});
	}


	public static void plotCsvColumns(String outputFolder, String name, String axisTitleX, String axisTitleY, ColumnToPlot[] columns)
			throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		for (ColumnToPlot col : columns) {
			dataset.addSeries(col.Table.getColumnDataset(col.ColumnIndex, col.OptionalName));
		}
		plotDataset(outputFolder, name, axisTitleX, axisTitleY, dataset);
	}

	public static void plotCsvColumns(String outputFolder, String name, String axisTitleX, String axisTitleY, List<ColumnToPlot> columns)
			throws IOException {
		final XYSeriesCollection dataset = new XYSeriesCollection();
		for (ColumnToPlot col : columns) {
			dataset.addSeries(col.Table.getColumnDataset(col.ColumnIndex, col.OptionalName));
		}
		plotDataset(outputFolder, name, axisTitleX, axisTitleY, dataset);
	}

	public static void plotCsvAllColumns(String outputFolder, String name, String axisTitleX, String axisTitleY, CsvDataFile data)
			throws IOException {
		plotDataset(outputFolder, name, axisTitleX, axisTitleY, data.getTableDatasets());
	}


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
		ChartUtilities.saveChartAsPNG(new File(outputFolder + File.separator + name + ".png"), chart, 1200, 800, info);
	}


	private static class ColumnToPlot {

		public final String OptionalName;
		public final CsvDataFile Table;
		public final int ColumnIndex;


		public ColumnToPlot(String optionalName, CsvDataFile table, int columnIndex) {
			super();
			OptionalName = optionalName;
			Table = table;
			ColumnIndex = columnIndex;
		}
	}



	public static void main(String[] args) {
		try {
			plotStats("output" + File.separator + "stats");
			System.out.println("Plot finished");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
