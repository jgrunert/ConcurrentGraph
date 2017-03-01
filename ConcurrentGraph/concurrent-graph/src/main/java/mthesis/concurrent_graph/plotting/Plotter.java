package mthesis.concurrent_graph.plotting;

import java.awt.Color;
import java.io.File;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;

public class Plotter {

	public static void plotStats(String outputFolder) throws Exception {
		String statsFolder = outputFolder + File.separator + "stats";
		CsvDataFile activeVerts = new CsvDataFile(statsFolder + File.separator + "0_times_ms.csv");
		plotCsv(activeVerts, "test.png");
	}

	public static void plotCsv(CsvDataFile data, String outFile) throws IOException {
		final JFreeChart chart = ChartFactory.createXYLineChart(
				"Line Chart Demo 6", // chart title
				"X", // x axis label
				"Y", // y axis label
				data.getColumnDatasets(), // data
				PlotOrientation.VERTICAL,
				true, // include legend
				true, // tooltips
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
		renderer.setSeriesLinesVisible(0, false);
		renderer.setSeriesShapesVisible(1, false);
		plot.setRenderer(renderer);

		// change the auto tick unit selection to integer units only...
		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		// OPTIONAL CUSTOMISATION COMPLETED.

		ChartRenderingInfo info = new ChartRenderingInfo(new StandardEntityCollection());
		ChartUtilities.saveChartAsPNG(new File(outFile), chart, 1200, 800, info);
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
