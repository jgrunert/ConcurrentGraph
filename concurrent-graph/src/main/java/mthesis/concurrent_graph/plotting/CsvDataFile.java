package mthesis.concurrent_graph.plotting;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class CsvDataFile {

	public final int NumDataColumns;
	public final int NumDataRows;
	public final String[] Captions;
	public final Map<String, Integer> ColumnNameIndex = new HashMap<>();
	public final double[][] Data;

	public CsvDataFile(String file, int samplingFactor) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Captions = reader.readLine().split(";");
		NumDataColumns = Captions.length;

		for (int i = 0; i < Captions.length; i++) {
			ColumnNameIndex.put(Captions[i], i);
		}

		List<double[]> dataLinesRead = new ArrayList<>();
		String line;
		while ((line = reader.readLine()) != null) {
			String[] lineSplit = line.split(";");
			double[] dataLine = new double[NumDataColumns];
			for (int i = 0; i < NumDataColumns && i < lineSplit.length; i++) {
				dataLine[i] = Double.parseDouble(lineSplit[i]);
			}
			dataLinesRead.add(dataLine);
		}

		List<double[]> dataLines;
		if (samplingFactor <= 1) {
			dataLines = dataLinesRead;
		}
		else {
			dataLines = new ArrayList<>();
			for (int iLine = 0; iLine < dataLinesRead.size(); iLine++) {
				int iSample = 1;
				double[] sum = dataLinesRead.get(iLine);
				for (; iSample < samplingFactor && iLine + 1 < dataLinesRead.size(); iSample++) {
					iLine++;
					double[] sample = dataLinesRead.get(iLine);
					for (int iCol = 0; iCol < sum.length; iCol++) {
						sum[iCol] += sample[iCol];
					}
				}
				for (int iCol = 0; iCol < sum.length; iCol++) {
					sum[iCol] /= iSample;
				}
				dataLines.add(sum);
			}
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
			dataset.addSeries(getColumnDataset(iCol, 1.0, null, 0));
		}

		return dataset;
	}

	public XYSeries getColumnDataset(int columnIndex, double factor, String optionalName, int startRow) {
		if (optionalName == null)
			optionalName = Captions[columnIndex];
		final XYSeries series = new XYSeries(optionalName);
		for (int iRow = startRow; iRow < NumDataRows; iRow++) {
			series.add(iRow, Data[iRow][columnIndex] * factor);
		}
		return series;
	}


	public int getClumnIndexByName(String colName) {
		return ColumnNameIndex.get(colName);
	}

	public double getValueByName(String colName, int rowIndex) {
		int colIndex = getClumnIndexByName(colName);
		return Data[rowIndex][colIndex];
	}
}
