package mthesis.concurrent_graph.util;

import java.io.File;

public class FileUtil {

	public static void makeCleanDirectory(String dir) {
		final File dirFile = new File(dir);
		if (dirFile.exists()) {
			for (final File f : dirFile.listFiles()) {
				deleteFile(f);
			}
		}
		else {
			dirFile.mkdirs();
		}
	}

	public static void deleteFile(File file) {
		if (file.isDirectory()) {
			for (final File f : file.listFiles())
				deleteFile(f);
		}
		file.delete();
	}
}
