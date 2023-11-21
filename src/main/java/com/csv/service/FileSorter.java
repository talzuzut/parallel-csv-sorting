package com.csv.service;

import com.csv.config.FileSorterArgs;
import com.csv.model.ChunkDetails;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.csv.util.FileUtil.getChunkFileName;
import static com.csv.util.FileUtil.readChunk;

public class FileSorter {

	public int splitToSortedRecordsChunks(FileSorterArgs args) throws IOException, CsvValidationException {
		int chunkNumber = 0;
		try (CSVReader csvReader = new CSVReader(new FileReader(args.inputFileName))) {
			ExecutorService executorService = Executors.newFixedThreadPool(args.numThreads);
			List<CompletableFuture<Void>> futures = new ArrayList<>();

			while (true) {
				List<List<String>> recordsChunk = readChunk(args.maxRecordsInMemory, csvReader);
				if (recordsChunk.isEmpty()) {
					break;
				}
				// If the file is small enough to fit in memory, sort it in memory
				if (recordsChunk.size() < args.maxRecordsInMemory) {
					sortAllFileInOneChunk(args, recordsChunk, executorService, futures);
					return 1;
				}
				List<List<String>> sortedRecordsChunk = sortChunk(args.keyFieldIndex, recordsChunk);
				String chunkFileName = getChunkFileName(chunkNumber, 0);
				CompletableFuture<Void> future = CompletableFuture.runAsync(() -> FileUtil.writeChunkToFile(sortedRecordsChunk, chunkFileName), executorService);
				futures.add(future);

				chunkNumber++;
			}

			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
			executorService.shutdown();
		}

		return chunkNumber;
	}

	private void sortAllFileInOneChunk(FileSorterArgs args, List<List<String>> recordsChunk, ExecutorService executorService, List<CompletableFuture<Void>> futures) {
		List<List<String>> sortedRecordsChunk = sortChunk(args.keyFieldIndex, recordsChunk);
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> FileUtil.writeChunkToFile(sortedRecordsChunk, args.outputFileName), executorService);
		futures.add(future);
		CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
		executorService.shutdown();
		FileUtil.createFinalOutputFile(args, 0);
	}

	private List<List<String>> sortChunk(int keyFieldIndex, List<List<String>> chunk) {
		List<List<String>> sortedChunk = new ArrayList<>(chunk);
		sortedChunk.sort(Comparator.comparing(o -> o.get(keyFieldIndex)));
		return sortedChunk;
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
		ExecutorService executorService = Executors.newFixedThreadPool(args.numThreads);

		while (totalChunks > 1) {
			List<CompletableFuture<Void>> mergeFutures = new ArrayList<>();
			int currentPassChunkNumber = 0;

			// Split merging into groups of maxRecordsInMemory
			for (int index = 0; index < totalChunks; index += args.maxRecordsInMemory) {
				int endIndex = Math.min(index + args.maxRecordsInMemory, totalChunks);
				List<CompletableFuture<Void>> chunkMergeFutures = new ArrayList<>();

				int finalPassNumber = passNumber;
				int finalIndex = index;
				int finalCurrentPassChunkNumber = currentPassChunkNumber;

				// Merge chunks within a group
				CompletableFuture<Void> chunkMergeFuture = CompletableFuture.runAsync(() -> {
					try {
						ChunkDetails chunkDetails = new ChunkDetails(finalPassNumber, finalIndex, endIndex, finalCurrentPassChunkNumber);
						mergeChunksByRecordsLimit(args, chunkDetails);
					} catch (IOException | CsvValidationException e) {
						throw new RuntimeException(e);
					}
				}, executorService);

				chunkMergeFutures.add(chunkMergeFuture);

				CompletableFuture<Void> groupMergeFuture = CompletableFuture.allOf(chunkMergeFutures.toArray(new CompletableFuture[0]));
				mergeFutures.add(groupMergeFuture);
				currentPassChunkNumber++;
			}

			CompletableFuture.allOf(mergeFutures.toArray(new CompletableFuture[0])).join();

			totalChunks = (int) Math.ceil((double) totalChunks / args.maxRecordsInMemory);
			passNumber++;
			if (totalChunks == 1) {
				FileUtil.createFinalOutputFile(args, passNumber);
			}
		}

		executorService.shutdown();
	}

	private void mergeChunksByRecordsLimit(FileSorterArgs args, ChunkDetails chunkDetails) throws IOException, CsvValidationException {
		PriorityQueue<RecordPointer> minHeap = new PriorityQueue<>(Comparator.comparing(o -> o.getCurrentRecord().get(args.keyFieldIndex)));
		int passNumber = chunkDetails.getPassNumber();

		// Initialize the heap with the first record from each remaining chunk
		for (int i = chunkDetails.getStartIndex(); i < chunkDetails.getEndIndex(); i++) {
			String chunkFileName = getChunkFileName(i, passNumber);
			CSVReader csvReader = new CSVReader(new FileReader(chunkFileName));
			RecordPointer recordPointer = new RecordPointer(csvReader);
			List<String> currentRecord = recordPointer.getCurrentRecord();

			if (currentRecord != null) {
				minHeap.add(recordPointer);
			}
		}

		String outputFileName = getChunkFileName(chunkDetails.getChunkNumber(), passNumber + 1);
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
	}
}
