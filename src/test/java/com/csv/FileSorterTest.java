package com.csv;

import com.csv.config.FileSorterArgs;
import com.csv.service.FileSorter;
import com.csv.util.FileUtil;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FileSorterTest {
	private static final int MAX_RECORDS_IN_MEMORY = 10;
	private static final String[] ARGS = {"0", String.valueOf(MAX_RECORDS_IN_MEMORY), "input.csv", "output.csv", "5"};
	private static final FileSorterArgs FILE_SORTER_ARGS = new FileSorterArgs(ARGS);
	private static final int INPUT_LINES = 3 * MAX_RECORDS_IN_MEMORY;
	private static final String TEMP_FOLDER = "temp_test";
	private static List<List<String>> SHUFFLED_RECORDS;
	private static List<List<String>> RECORDS;
	private static List<List<String>> SORTED_CHUNK_1, SORTED_CHUNK_2, SORTED_CHUNK_3;
	private static ArrayList<List<List<String>>> SORTED_CHUNKS;
	@Mock
	private FileUtil fileUtilMock;


	@InjectMocks
	private FileSorter fileSorter;

	private static void writeSortedChunksToFile() {
		//chunks should be inner sorted, but not compared to each other
		// meaning chunk1 can have bigger and smaller values than chunk2
		RECORDS = new ArrayList<>();
		int numberOfChunks = 3;
		for (int i = 0; i < FILE_SORTER_ARGS.maxRecordsInMemory * numberOfChunks; i++) {
			String id = String.format("b%03d", i + 1);
			String name = "Name" + (i + 1);
			RECORDS.add(Arrays.asList(id, name));
		}
		SHUFFLED_RECORDS = new ArrayList<>(RECORDS);
		Collections.shuffle(SHUFFLED_RECORDS);
		List<List<String>> Chunk1 = SHUFFLED_RECORDS.subList(0, FILE_SORTER_ARGS.maxRecordsInMemory);
		SORTED_CHUNK_1 = sortChunk(FILE_SORTER_ARGS.keyFieldIndex, Chunk1);
		List<List<String>> Chunk2 = SHUFFLED_RECORDS.subList(FILE_SORTER_ARGS.maxRecordsInMemory, 2 * FILE_SORTER_ARGS.maxRecordsInMemory);
		SORTED_CHUNK_2 = sortChunk(FILE_SORTER_ARGS.keyFieldIndex, Chunk2);
		List<List<String>> Chunk3 = SHUFFLED_RECORDS.subList(2 * FILE_SORTER_ARGS.maxRecordsInMemory, 3 * FILE_SORTER_ARGS.maxRecordsInMemory);
		SORTED_CHUNK_3 = sortChunk(FILE_SORTER_ARGS.keyFieldIndex, Chunk3);
		SORTED_CHUNKS = new ArrayList<>(Arrays.asList(SORTED_CHUNK_1, SORTED_CHUNK_2, SORTED_CHUNK_3));
	}

	public static List<List<String>> sortChunk(int keyFieldIndex, List<List<String>> chunk) {
		return chunk.stream().sorted(Comparator.comparing(o -> o.get(keyFieldIndex))).collect(Collectors.toList());
	}

	@BeforeAll
	static void setUp() {

		Main.tempFolder = new File(TEMP_FOLDER);
		Main.tempFolder.mkdir();
		writeSortedChunksToFile();
		if (SHUFFLED_RECORDS.size() < FILE_SORTER_ARGS.maxRecordsInMemory) {
			FILE_SORTER_ARGS.maxRecordsInMemory = SHUFFLED_RECORDS.size();
		}

	}

	private String getUniqueTempFolder() {
		return TEMP_FOLDER + "_" + System.currentTimeMillis();
	}

	@AfterEach
	void tearDown() {
		FileUtil fileUtil = new FileUtil();
		fileUtil.deleteFolder(Main.tempFolder);
		//delete output file, comment this block if you want to see the output file
		File file = new File(FILE_SORTER_ARGS.outputFileName);
		if (file.exists()) {
			file.delete();
		}
	}

	@Test
	void givenInputFile_whenSplitToSortedRecordsChunks_thenChunksAreSorted() throws Exception {
		CSVReader csvReaderMock = mock(CSVReader.class);
		when(fileUtilMock.createCSVReader(FILE_SORTER_ARGS.inputFileName)).thenReturn(csvReaderMock);
		when(fileUtilMock.readChunk(FILE_SORTER_ARGS.maxRecordsInMemory, csvReaderMock)).thenAnswer(new Answer<List<List<String>>>() {
			private int count = 0;

			public List<List<String>> answer(InvocationOnMock invocation) {
				if (count < INPUT_LINES) {
					List<List<String>> chunk = new ArrayList<>();
					for (int i = 0; i < FILE_SORTER_ARGS.maxRecordsInMemory && count < INPUT_LINES; i++) {
						chunk.add(SHUFFLED_RECORDS.get(count++));
					}
					return chunk;
				} else {
					return Collections.emptyList();
				}
			}
		});

		//assert that the chunk is sorted
		doAnswer((Answer<Void>) invocation -> {
			List<List<String>> chunk = invocation.getArgument(0);
			List<List<String>> sortedChunk = new ArrayList<>(chunk);
			sortedChunk.sort(Comparator.comparing(o -> o.get(FILE_SORTER_ARGS.keyFieldIndex)));

			assertEquals(sortedChunk, chunk, "Chunk is not sorted");

			return null;
		}).when(fileUtilMock).writeChunkToFile(anyList(), any());
		int sortedChunksCount = fileSorter.splitToSortedRecordsChunks(FILE_SORTER_ARGS);
		int expectedSortedChunksCount = (int) Math.ceil((double) INPUT_LINES / FILE_SORTER_ARGS.maxRecordsInMemory);
		assertAll(() -> assertEquals(expectedSortedChunksCount, sortedChunksCount), () -> verify(fileUtilMock, times(expectedSortedChunksCount)).writeChunkToFile(anyList(), any()));

	}

	private void writeSortedChunksToFile(FileUtil fileUtil, String outputFolder) {

		fileUtil.writeChunkToFile(SORTED_CHUNK_1, outputFolder + "/pass_0_chunk_0.csv");
		fileUtil.writeChunkToFile(SORTED_CHUNK_2, outputFolder + "/pass_0_chunk_1.csv");
		fileUtil.writeChunkToFile(SORTED_CHUNK_3, outputFolder + "/pass_0_chunk_2.csv");
	}

	@Test
	void givenChunkGroup_whenMergeAllSortedChunks_andRequireOnePassMerge_thenOutputFileCreated() throws CsvValidationException, IOException {
		//Note mocks are not used here, we are using the actual fileUtil and fileSorter objects
		MergeAllSortedChunksResult result = getMergeAllSortedChunksResult();
		List<String> outputFileContent = Files.readAllLines(Paths.get(result.customArgs.outputFileName));
		List<String> expectedContent = RECORDS.stream().map(list -> String.join(",", list)).collect(Collectors.toList());
		assertEquals(outputFileContent, expectedContent);
		assert (Files.exists(Paths.get(result.customArgs.outputFileName)));
		result.fileUtil.deleteFolder(result.outputFolderFile);
	}

	@Test
	void givenChunkGroup_whenMergeAllSortedChunks_andRequireMultiplePassMerge_thenOutputFileCreated() throws CsvValidationException, IOException {
		//Note mocks are not used here, we are using the actual fileUtil and fileSorter objects
		MergeAllSortedChunksResult result = getMergeAllSortedChunksResult();

		assertEquals(Files.readAllLines(Paths.get(result.customArgs.outputFileName)), RECORDS.stream().map(list -> String.join(",", list)).collect(Collectors.toList()));
		assert (Files.exists(Paths.get(result.customArgs.outputFileName)));
		result.fileUtil.deleteFolder(result.outputFolderFile);

	}

	private MergeAllSortedChunksResult getMergeAllSortedChunksResult() throws IOException, CsvValidationException {
		String outputFolder = getUniqueTempFolder();
		FileUtil fileUtil = new FileUtil();
		File outputFolderFile = new File(outputFolder);
		outputFolderFile.mkdir();
		String customOutputFileName = outputFolder + "/output.csv";
		FileSorterArgs customArgs = new FileSorterArgs(ARGS);
		customArgs.outputFileName = customOutputFileName;
		customArgs.tempFolder = outputFolder;
		writeSortedChunksToFile(fileUtil, outputFolder);
		FileSorter fileSorter = new FileSorter(fileUtil);
		fileSorter.mergeAllSortedChunks(customArgs, SORTED_CHUNKS.size());
		return new MergeAllSortedChunksResult(fileUtil, outputFolderFile, customArgs);
	}


}
