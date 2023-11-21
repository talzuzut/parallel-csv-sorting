package com.csv.model;

public class ChunkDetails {
	private final int passNumber;
	private final int startIndex;
	private final int endIndex;
	private final int chunkNumber;

	public ChunkDetails(int passNumber, int startIndex, int endIndex, int chunkNumber) {
		this.passNumber = passNumber;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.chunkNumber = chunkNumber;
	}

	public int getPassNumber() {
		return passNumber;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	public int getChunkNumber() {
		return chunkNumber;
	}
}
