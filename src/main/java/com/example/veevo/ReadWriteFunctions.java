package com.example.veevo;

import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class ReadWriteFunctions {

	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(ReadWriteFunctions.class);
	
	/**
	 * 
	 * This is a function to manage the process of reading CSV file,
	 * read it as blocks based on memory limits, sort the blocks records and write to smaller CSV files. 
	 *
	 * @return - returns the number of smaller CSV files written.
	 * @throws IOException
	 * @throws CsvValidationException
	 * @throws URISyntaxException
	 */
	public static int processFileAsBlocks() throws IOException, CsvValidationException, URISyntaxException {
		
		
		Path filePath = Paths.get(
				ClassLoader.getSystemResource(Consts.INPUT_FILE).toURI()
				);
		
		// Using try with resources will close the declared resource when the block is done or in case of exception. 
		// The closing order will be in the opposite of the deceleration order.
		try (Reader reader = Files.newBufferedReader(filePath); CSVReader csvReader = new CSVReader(reader)) {
			
			logger.trace("File size: " + Files.size(filePath));
			
			long blockSize = calcBlockSize();
			
			String[] line = csvReader.readNext();
			Long lineSize = recordSize(line);
					
			List<String[]> partialFile = new ArrayList<String[]>();
			long accumulatedRecordsSize = 0;
			
			int partialFileIndex = 0;
			
			while (line != null) {
				
				// In case there is really big line in file, which exceed the memory limit.
				if(lineSize >= blockSize) {
					logger.error("Not enough memory to process file. Please increase memory and try again.");
					System.exit(-1);
				}
				
				while(accumulatedRecordsSize + lineSize < blockSize) {
					partialFile.add(line);
					accumulatedRecordsSize += lineSize;
					
					line = csvReader.readNext();
					lineSize = recordSize(line);
					if(line == null) {
						logger.trace("Reached to the end of the CSVReader.");
						break;
					}
				}
				
				// We will get here when we reached the limit of block size.
				// At his point we would like to sort the records and write them to small sorted file.

				partialFileIndex++;
				logger.trace("Number of partial files: " + partialFileIndex);

				// Creating new thread to sort and write the partial file.
				SortingThread s = new SortingThread(partialFile, partialFileIndex);
				s.run();

				partialFile.clear();
				accumulatedRecordsSize = 0;
				
			}
			
			logger.info("Number of partial files is: " + partialFileIndex);
			return partialFileIndex;
		}
		
	}
	
	/**
	 * Calculate the provided record size in bytes.
	 * Since Java chars are 2 bytes, the record size should be the the string length multiplied by 2.
	 * @param record - the record to calculate its size.
	 * @return - the size of the provided record in bytes.
	 */
	private static long recordSize(String[] record) {
		if(record == null) {
			return 0;
		}
		String joinedString = String.join(",", record);
		return joinedString.length()*2;
	}
	
	/**
	 * calculate the size of block based on the free memory and the memory limit.
	 * @return the block size.
	 */
	public static long calcBlockSize() {
		 Runtime r = Runtime.getRuntime();
		 long freeMemory = r.freeMemory();
		 
		 logger.info("Free memory: " + freeMemory);
		 
		 long memoryLimit = Consts.MEMORY_LIMIT;
		 long blockSize = freeMemory/memoryLimit;
		 
		 logger.info("Block size: " + blockSize);
		 
		 return blockSize;
	}
	
}
