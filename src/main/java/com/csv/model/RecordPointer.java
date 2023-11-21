package com.csv.model;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RecordPointer {
	private final CSVReader csvReader;
	private List<String> currentRecord;

	public RecordPointer(CSVReader csvReader) throws CsvValidationException, IOException {
		this.csvReader = csvReader;
		this.currentRecord = getNextRecord();
	}

	public List<String> getCurrentRecord() {
		return currentRecord;
	}

	public List<String> getNextRecord() throws IOException, CsvValidationException {
		String[] values = csvReader.readNext();
		if (values != null) {
			List<String> record = Arrays.asList(values);
			this.currentRecord = record;
			return record;
		} else {
			csvReader.close();
			return null;
		}
	}
}
