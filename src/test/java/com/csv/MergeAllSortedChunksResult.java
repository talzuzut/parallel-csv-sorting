package com.csv;

import com.csv.config.FileSorterArgs;
import com.csv.util.FileUtil;

import java.io.File;

public  class MergeAllSortedChunksResult {
	public final FileUtil fileUtil;
	public final File outputFolderFile;
	public final FileSorterArgs customArgs;

	public MergeAllSortedChunksResult(FileUtil fileUtil, File outputFolderFile, FileSorterArgs customArgs) {
		this.fileUtil = fileUtil;
		this.outputFolderFile = outputFolderFile;
		this.customArgs = customArgs;
	}
}
