package mthesis.concurrent_graph.util;

import java.io.File;

public class FileUtil {

	public static void createDirOrEmptyFiles(String dir) {
		final File dirFile = new File(dir);
		if (dirFile.exists()) {
			for (final File f : dirFile.listFiles()) {
				emptyDirectory(f);
			}
		}
		else {
			dirFile.mkdirs();
		}
	}

	public static void createEmptyDir(String dir) {
		final File dirFile = new File(dir);
		if (dirFile.exists()) {
			for (final File f : dirFile.listFiles()) {
				deleteDirRecursive(f);
			}
		}
		else {
			dirFile.mkdirs();
		}
	}

	public static void deleteDirRecursive(File file) {
		if (file.isDirectory()) {
			for (final File f : file.listFiles())
				deleteDirRecursive(f);
		}
		file.delete();
	}

	public static void emptyDirectory(File file) {
		if (file.isDirectory()) {
			for (final File f : file.listFiles())
				emptyDirectory(f);
		}
		else {
			file.delete();
		}
	}
}
