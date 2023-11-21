package com.csv;

import com.csv.config.FileSorterArgs;
import com.csv.service.FileSorter;

import java.io.File;

import static com.csv.util.FileUtil.createTempFolder;
import static com.csv.util.FileUtil.deleteFolder;

public class Main {
	public static File tempFolder;

	public static void main(String[] args) {
		tempFolder = createTempFolder();

		FileSorterArgs fileSorterArgs = new FileSorterArgs(args);
		FileSorter fileSorter = new FileSorter();
		fileSorter.externalMergeSortFile(fileSorterArgs);

		deleteFolder(tempFolder);
	}

}