package com.csv.util;


import com.csv.Main;
import com.csv.config.FileSorterArgs;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtil {
	public final static String TEMP_DIR = "temp";

	public CSVReader createCSVReader(String inputFileName) throws FileNotFoundException {
		return new CSVReader(new FileReader(inputFileName));
	}

	public CSVWriter createCSVWriter(String outputFileName) throws IOException {
		return (CSVWriter) new CSVWriterBuilder(new FileWriter(outputFileName))
				.withQuoteChar(CSVWriter.NO_QUOTE_CHARACTER)
				.build();
	}

	public File createTempFolder() {
		String projectDirectory = System.getProperty("user.dir");
		File tempFolder = new File(projectDirectory, TEMP_DIR);
		if (tempFolder.exists()) {
			deleteFolder(tempFolder);
		}
		if (!tempFolder.mkdir()) {
			throw new RuntimeException("Cannot create temp folder");
		}

		return tempFolder;
	}

	public void deleteFolder(File folder) {
		if (folder.isDirectory()) {
			File[] files = folder.listFiles();
			if (files != null) {
				for (File file : files) {
					deleteFolder(file);
				}
			}
		}
		folder.delete();
	}

	public List<List<String>> readChunk(int maxRecordsInMemory, CSVReader csvReader) throws IOException, CsvValidationException {
		List<List<String>> records = new ArrayList<>();
		for (int i = 0; i < maxRecordsInMemory; i++) {
			String[] values = csvReader.readNext();
			if (values != null) {
				records.add(Arrays.asList(values));
			} else {
				break;
			}
		}
		return records;
	}


	public void writeChunkToFile(List<List<String>> fileChunk, String chunkFileName) {
		try (CSVWriter csvWriter = createCSVWriter(chunkFileName)) {
			List<String[]> data = fileChunk.stream()
					.map(list -> list.toArray(new String[0]))
					.collect(Collectors.toList());
			csvWriter.writeAll(data);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getChunkFileName(FileSorterArgs args, int chunkNumber, int passNumber) {
		String tempFolderName = args.tempFolder != null ? args.tempFolder : Main.tempFolder.getName();
		return tempFolderName + File.separator + "pass_" + passNumber + "_chunk_" + chunkNumber + ".csv";
	}

	public void createFinalOutputFile(FileSorterArgs args, int passNumber) {
		String projectDirectory = System.getProperty("user.dir");
		String finalOutputFileName = args.outputFileName;

		Path outputPath = Paths.get(projectDirectory, finalOutputFileName);

		String chunkFileName = getChunkFileName(args, 0, passNumber);
		File chunkFile = new File(chunkFileName);

		if (chunkFile.exists()) {
			chunkFile.renameTo(outputPath.toFile());
		} else {
			System.err.println("Error: Chunk file not found.");
		}
	}
}
