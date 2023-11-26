package com.csv.util;

import com.csv.Main;
import com.csv.config.FileSorterArgs;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static com.csv.util.FileUtil.*;
import static org.junit.jupiter.api.Assertions.*;

public class FileUtilTest {

	private static final String TEMP_DIR = "temp";
	private static final String OUTPUT_FILE_NAME = "output.csv";
	private static final String[] ARGS = {"0", "50", "input.csv", OUTPUT_FILE_NAME, "5"};
	private static final String TEST_FILE_NAME = "test.csv";
	private static final List<String> FIRST_RECORD = Arrays.asList("a001", "John");
	private static final List<String> SECOND_RECORD = Arrays.asList("a002", "Jane");
	private static final List<String> THIRD_RECORD = Arrays.asList("a003", "Doe");
	private static final List<List<String>> TEST_DATA = Arrays.asList(
			FIRST_RECORD,
			SECOND_RECORD,
			THIRD_RECORD
	);
	private static final FileUtil fileUtil = new FileUtil();

	@BeforeAll
	static void setUp() {
		Main.tempFolder = new File(TEMP_DIR);

	}

	@AfterAll
	static void tearDown() {
		fileUtil.deleteFolder(Main.tempFolder);
		fileUtil.deleteFolder(new File(OUTPUT_FILE_NAME));
	}
	@AfterEach
	void cleanUp() {
	}

	@Test
	void givenNoTempFolderExists_whenCreateTempFolder_thenTempFolderCreated() {
		fileUtil.deleteFolder(Main.tempFolder);
		assertFalse(Main.tempFolder.exists());
		fileUtil.createTempFolder();
		assertAll(
				() -> assertTrue(Main.tempFolder.exists()),
				() -> assertTrue(Main.tempFolder.isDirectory())
		);
	}

	@Test
	void givenTempFolderExistsWithFiles_whenDeleteFolder_thenFolderAndFilesDeleted() {
		File tempFolder = fileUtil.createTempFolder();
		File testFile = new File(tempFolder, "test.txt");

		assertAll(
				() -> assertTrue(testFile.createNewFile()),
				() -> assertTrue(testFile.exists()),
				() -> assertTrue(tempFolder.exists()),
				() -> {
					fileUtil.deleteFolder(tempFolder);
					assertFalse(testFile.exists());
					assertFalse(tempFolder.exists());
				}
		);
	}

	@Test
	void givenCsvFileWithRecords_whenReadChunk_thenRecordsReadCorrectly() throws IOException, CsvValidationException {
		File tempFolder = fileUtil.createTempFolder();
		String testFilePath = tempFolder.getAbsolutePath() + File.separator + TEST_FILE_NAME;
		fileUtil.writeChunkToFile(TEST_DATA, testFilePath);
		try (CSVReader csvReader = new CSVReader(new FileReader(testFilePath))) {
			List<List<String>> records = fileUtil.readChunk(TEST_DATA.size(), csvReader);
			assertAll(
					() -> assertEquals(TEST_DATA.size(), records.size()),
					() -> assertEquals(FIRST_RECORD, records.get(0)),
					() -> assertEquals(SECOND_RECORD, records.get(1)),
					() -> assertEquals(THIRD_RECORD, records.get(2))
			);
		}
	}

	@Test
	void givenDataList_whenWriteChunkToFile_thenFileContainsCorrectData() throws IOException {
		File tempFolder = fileUtil.createTempFolder();
		String testFilePath = tempFolder.getAbsolutePath() + File.separator + TEST_FILE_NAME;
		fileUtil.writeChunkToFile(TEST_DATA, testFilePath);

		List<String> lines = Files.readAllLines(Paths.get(testFilePath));
		assertAll(
				() -> assertEquals(TEST_DATA.size(), lines.size()),
				() -> assertEquals(String.join(",", FIRST_RECORD), lines.get(0)),
				() -> assertEquals(String.join(",", SECOND_RECORD), lines.get(1)),
				() -> assertEquals(String.join(",", THIRD_RECORD), lines.get(2))
		);

	}

	@Test
	void givenChunkNumberAndPassNumber_whenGetChunkFileName_thenFileNameGeneratedCorrectly() {
		FileSorterArgs args = new FileSorterArgs(ARGS);
		Main.tempFolder = new File(TEMP_DIR);
		int chunkNumber = 1;
		int passNumber = 2;
		String chunkFileName = fileUtil.getChunkFileName(args,1, 2);
		assertEquals(TEMP_DIR + File.separator + "pass_" + passNumber + "_chunk_" + chunkNumber + ".csv", chunkFileName);	}

	@Test
	void givenTemporaryChunkFile_whenCreateFinalOutputFile_thenFileIsRenamed() throws IOException {
		File tempFolder = fileUtil.createTempFolder();
		String outputFileName = "output.csv";
		FileSorterArgs args = new FileSorterArgs(FileUtilTest.ARGS);
		args.outputFileName = outputFileName;

		// Create a temporary chunk file
		String tempChunkFileName = fileUtil.getChunkFileName(args,0, 1);
		Path tempChunkFilePath = Paths.get( tempChunkFileName);
		Files.createFile(tempChunkFilePath);

		// Call the method to create the final output file
		fileUtil.createFinalOutputFile(args, 1);

		// Check if the output file exists and the temporary chunk file is renamed
		assertTrue(Files.exists(Paths.get(outputFileName))); // Output file should exist
	}
}
