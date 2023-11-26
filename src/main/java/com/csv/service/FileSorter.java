package com.csv.service;

import com.csv.config.FileSorterArgs;
import com.csv.model.ChunkGroupDetails;
import com.csv.model.RecordPointer;
import com.csv.util.FileUtil;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FileSorter {
	private final FileUtil fileUtil;

	public FileSorter(FileUtil fileUtil) {
		this.fileUtil = fileUtil;
	}

	public int splitToSortedRecordsChunks(FileSorterArgs args) throws IOException, CsvValidationException {
		int chunkNumber = 0;
		try (CSVReader csvReader = fileUtil.createCSVReader(args.inputFileName)) {
			while (true) {
				List<List<String>> recordsChunk = fileUtil.readChunk(args.maxRecordsInMemory, csvReader);
				if (recordsChunk.isEmpty()) {
					break;
				}
				// If the file is small enough to fit in memory, sort it in memory
				if (recordsChunk.size() < args.maxRecordsInMemory) {
					sortAllFileInOneChunk(args, recordsChunk);
					return 1;
				}
				List<List<String>> sortedRecordsChunk = sortChunk(args.keyFieldIndex, recordsChunk);
				String chunkFileName = fileUtil.getChunkFileName(args, chunkNumber, 0);
				fileUtil.writeChunkToFile(sortedRecordsChunk, chunkFileName);

				chunkNumber++;
			}

		}

		return chunkNumber;
	}

	public void sortAllFileInOneChunk(FileSorterArgs args, List<List<String>> recordsChunk) {
		List<List<String>> sortedRecordsChunk = sortChunk(args.keyFieldIndex, recordsChunk);
		fileUtil.writeChunkToFile(sortedRecordsChunk, args.outputFileName);
	}

	private List<List<String>> sortChunk(int keyFieldIndex, List<List<String>> chunk) {
		chunk.sort(Comparator.comparing(o -> o.get(keyFieldIndex)));
		return chunk;
	}

	public void externalMergeSortFile(FileSorterArgs args) {
		try {
			int sortedChunksCount = splitToSortedRecordsChunks(args);
			mergeAllSortedChunks(args, sortedChunksCount);
		} catch (CsvValidationException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void mergeAllSortedChunks(FileSorterArgs args, int initialTotalChunks) throws IOException, CsvValidationException {
		int passNumber = 0;
		int totalChunks = initialTotalChunks;

		while (totalChunks > 1) {
			int currentPassChunkNumber = 0;

			for (int index = 0; index < totalChunks; index += args.maxRecordsInMemory) {
				int endIndex = Math.min(index + args.maxRecordsInMemory, totalChunks);

				// Merge chunks within a group
				ChunkGroupDetails chunkGroupDetails = new ChunkGroupDetails(passNumber, index, endIndex, currentPassChunkNumber);
				mergeChunkGroupByRecordsLimit(args, chunkGroupDetails);

				currentPassChunkNumber++;
			}

			totalChunks = (int) Math.ceil((double) totalChunks / args.maxRecordsInMemory);
			passNumber++;
			if (totalChunks == 1) {
				fileUtil.createFinalOutputFile(args, passNumber);
			}
		}
	}

	private void mergeChunkGroupByRecordsLimit(FileSorterArgs args, ChunkGroupDetails chunkGroupDetails) throws IOException, CsvValidationException {
		PriorityQueue<RecordPointer> minHeap = new PriorityQueue<>(Comparator.comparing(o -> o.getCurrentRecord().get(args.keyFieldIndex)));
		int passNumber = chunkGroupDetails.getPassNumber();
		ExecutorService executor = createExecutorService(args.numThreads);

		// Initialize the heap with the first record from each remaining chunk
		List<Future<RecordPointer>> futures = new ArrayList<>();
		for (int i = chunkGroupDetails.getStartIndex(); i < chunkGroupDetails.getEndIndex(); i++) {
			final int index = i;
			futures.add(executor.submit(() -> {
				String chunkFileName = fileUtil.getChunkFileName(args, index, passNumber);
				CSVReader csvReader = new CSVReader(new FileReader(chunkFileName));
				RecordPointer recordPointer = new RecordPointer(csvReader);
				List<String> currentRecord = recordPointer.getCurrentRecord();
				return currentRecord != null ? recordPointer : null;
			}));
		}

		for (Future<RecordPointer> future : futures) {
			try {
				RecordPointer recordPointer = future.get();
				if (recordPointer != null) {
					minHeap.add(recordPointer);
				}
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		String outputFileName = fileUtil.getChunkFileName(args, chunkGroupDetails.getChunkNumber(), passNumber + 1);
		try (FileWriter writer = new FileWriter(outputFileName)) {
			while (!minHeap.isEmpty()) {
				RecordPointer smallest = minHeap.poll();
				List<String> smallestRecord = smallest.getCurrentRecord();

				writer.write(String.join(",", smallestRecord) + System.lineSeparator());

				List<String> nextRecord = smallest.getNextRecord();
				if (nextRecord != null) {
					minHeap.add(smallest);
				}
			}
		}
		executor.shutdown();
	}

	public ExecutorService createExecutorService(int numThreads) {
		return Executors.newFixedThreadPool(numThreads);
	}
}
