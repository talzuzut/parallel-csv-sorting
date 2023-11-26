package com.csv;

import com.csv.config.FileSorterArgs;
import com.csv.service.FileSorter;
import com.csv.util.FileUtil;

import java.io.File;

public class Main {
	public static File tempFolder;

	public static void main(String[] args) {
		FileUtil fileUtil = new FileUtil();
		tempFolder = fileUtil.createTempFolder();

		FileSorterArgs fileSorterArgs = new FileSorterArgs(args);
		FileSorter fileSorter = new FileSorter(fileUtil);
		fileSorter.externalMergeSortFile(fileSorterArgs);

		fileUtil.deleteFolder(tempFolder);
	}

}