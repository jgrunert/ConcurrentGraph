package mthesis.concurrent_graph.plotting;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class CsvDataFile {

	public final int NumDataColumns;
	public final int NumDataRows;
	public final String[] Captions;
	public final double[][] Data;

	public CsvDataFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Captions = reader.readLine().split(";");
		NumDataColumns = Captions.length;

		List<double[]> dataLines = new ArrayList<>();
		String line;
		while ((line = reader.readLine()) != null) {
			String[] lineSplit = line.split(";");
			double[] dataLine = new double[NumDataColumns];
			for (int i = 0; i < NumDataColumns; i++) {
				dataLine[i] = Double.parseDouble(lineSplit[i]);
			}
			dataLines.add(dataLine);
		}

		NumDataRows = dataLines.size();
		Data = new double[NumDataRows][];
		for (int i = 0; i < NumDataRows; i++) {
			Data[i] = dataLines.get(i);
		}

		reader.close();
	}


	/**
	 * @return A JFreeChart dataset for this csv files with all columns. X value incremented per line.
	 */
	public XYDataset getTableDatasets() {
		final XYSeriesCollection dataset = new XYSeriesCollection();

		for (int iCol = 0; iCol < NumDataColumns; iCol++) {
			dataset.addSeries(getColumnDataset(iCol, null));
		}

		return dataset;
	}

	public XYSeries getColumnDataset(int columnIndex, String optionalName) {
		if (optionalName == null)
			optionalName = Captions[columnIndex];
		final XYSeries series = new XYSeries(optionalName);
		for (int iRow = 0; iRow < NumDataRows; iRow++) {
			series.add(iRow, Data[iRow][columnIndex]);
		}
		return series;
	}
}
