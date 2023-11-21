package com.csv.config;

public class FileSorterArgs {
	public int keyFieldIndex;
	public int maxRecordsInMemory;
	public String inputFileName;
	public String outputFileName;
	public int numThreads;

	public FileSorterArgs(String[] args) {
		keyFieldIndex = Integer.parseInt(args[0]);
		maxRecordsInMemory = Integer.parseInt(args[1]);
		if (maxRecordsInMemory < 2) {
			throw new IllegalArgumentException("maxRecordsInMemory must be at least 2");
		}
		inputFileName = args[2];
		outputFileName = args[3];
		numThreads = Integer.parseInt(args[4]);
		if (numThreads < 1) {
			throw new IllegalArgumentException("numThreads must be at least 1");
		}
	}
}
