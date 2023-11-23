package com.csv.util;


import com.csv.Main;
import com.csv.config.FileSorterArgs;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtil {
	public final static String TEMP_DIR = "temp";

	public static File createTempFolder() {
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

	public static void deleteFolder(File folder) {
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

	public static List<List<String>> readChunk(int maxRecordsInMemory, CSVReader csvReader) throws IOException, CsvValidationException {
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


	public static void writeChunkToFile(List<List<String>> fileChunk, String chunkFileName) {

		try (CSVWriter csvWriter = new CSVWriter(new FileWriter(chunkFileName))) {
			List<String[]> data = fileChunk.stream()
					.map(list -> list.toArray(new String[0]))
					.collect(Collectors.toList());
			csvWriter.writeAll(data);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}


	}

	public static String getChunkFileName(int chunkNumber, int passNumber) {
		return Main.tempFolder.getName() + File.separator + "pass_" + passNumber + "_chunk_" + chunkNumber + ".csv";
	}

	public static void createFinalOutputFile(FileSorterArgs args, int passNumber) {
		String projectDirectory = System.getProperty("user.dir");
		String finalOutputFileName = args.outputFileName;

		Path outputPath = Paths.get(projectDirectory, finalOutputFileName);

		String chunkFileName = getChunkFileName(0, passNumber);
		File chunkFile = new File(chunkFileName);

		if (chunkFile.exists()) {
			chunkFile.renameTo(outputPath.toFile());
		} else {
			System.err.println("Error: Chunk file not found.");
		}
	}
}
