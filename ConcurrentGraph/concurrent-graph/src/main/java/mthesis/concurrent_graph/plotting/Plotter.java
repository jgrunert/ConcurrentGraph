package mthesis.concurrent_graph.plotting;

import java.io.File;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.data.general.DefaultPieDataset;

public class Plotter {

	public static void plotStats(String outputFolder) throws Exception {
		//Prepare the data set
		DefaultPieDataset pieDataset = new DefaultPieDataset();
		pieDataset.setValue("Coca-Cola", 26);
		pieDataset.setValue("Pepsi", 20);
		pieDataset.setValue("Gold Spot", 12);
		pieDataset.setValue("Slice", 14);
		pieDataset.setValue("Appy Fizz", 18);
		pieDataset.setValue("Limca", 10);

		//Create the chart
		JFreeChart chart = ChartFactory.createPieChart3D(
				"Soft Dink 3D Pie Chart", pieDataset, true, true, true);

		//Save chart as PNG
		ChartRenderingInfo info = new ChartRenderingInfo(new StandardEntityCollection());
		ChartUtilities.saveChartAsPNG(new File("soft3d.png"), chart, 400, 300, info);
	}



	public static void main(String[] args) {
		try {
			plotStats("");
			System.out.println("Plot finished");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
