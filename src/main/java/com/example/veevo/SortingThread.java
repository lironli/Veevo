package com.example.veevo;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

public class SortingThread implements Runnable {
	
	/**
	 * Partial file to be sorted by this thread
	 */
	private List<String[]> partialFile;
	
	/**
	 * The index of the current partial file
	 */
	private int fileIndex;
	
	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(SortingThread.class);
	
	/**
	 * Constructor with parameters.
	 * @param partialFileToSort - the records to be sorted and written to partial file.
	 * @param fileIndex - the index of the current partial file.
	 */
	public SortingThread(List<String[]> partialFileToSort, int fileIndex) {
		this.partialFile = partialFileToSort;
		this.fileIndex = fileIndex;
	}
	
	/**
	 * Main function of the thread.
	 * Sorts the records and write to file.
	 */
	public void run() {
		logger.info("Running thread for partial file with index: " + fileIndex);
		
		SortAndMergeFunctions.sort(partialFile);
		
		CSVWriter writer;
		try {
			writer = new CSVWriter(new FileWriter("src/main/resources/Sorted_part_" + fileIndex + ".csv"));
			writer.writeAll(partialFile);
			writer.close();
		} catch (IOException e) {
			logger.error("Failed to weite partial file with index: " + fileIndex);
			logger.error(e.toString());
		}
		
	}
}
