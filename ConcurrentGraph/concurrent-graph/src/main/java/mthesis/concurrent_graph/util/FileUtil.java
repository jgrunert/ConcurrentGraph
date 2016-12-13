package mthesis.concurrent_graph.util;

import java.io.File;

public class FileUtil {
	public static void makeCleanDirectory(String dir) {
		final File outFile = new File(dir);
		if(outFile.exists())
			for(final File f : outFile.listFiles())
				f.delete();
		else
			outFile.mkdirs();
	}
}
