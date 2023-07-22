package com.example.veevo;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

public class SortAndMergeFunctions {
	
	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(SortAndMergeFunctions.class);
	
	/**
	 * Sorts the provided list of records.
	 * @param unsortedRecords the records to be sorted.
	 * @return the records in sorted order.
	 */
	public static List<String[]> sort (List<String[]> unsortedRecords) {
		
		unsortedRecords.sort(compareRecords);
		return unsortedRecords;
	}
	
	/**
	 * Comparator to be used to sort chunks of records
	 */
	private static Comparator<String[]> compareRecords = new Comparator<String[]>() {
		@Override
		public int compare(final String[] record1, final String[] record2) {
            return record1[Consts.SORT_BY_COL].compareTo(record2[Consts.SORT_BY_COL]);
        }
     };
     
     /**
      * Comparator of 2 CSVReader based on the current line to be read.
      */
     private static Comparator<CSVReader>  compareReaders= new Comparator<CSVReader>(){
 		@Override
 		public int compare(final CSVReader reader1, final CSVReader reader2) {
             try {
				return reader1.peek()[Consts.SORT_BY_COL].compareTo(reader2.peek()[Consts.SORT_BY_COL]);
			} catch (IOException e) {
				logger.error("Failed to compare the provided readers");
				logger.error(e.toString());
				return 0;
			}
         }
      };
      
      /**
       * Merges the partial files into one file in sorted order.
       * @param numberOfFiles - the number of partial files to sort.
       * @throws URISyntaxException
       * @throws IOException
       * @throws CsvValidationException
       */
      public static void merge(int numberOfFiles) throws URISyntaxException, IOException, CsvValidationException {
    	  PriorityQueue<CSVReader> queue = loadReadersToPriorityQueue(numberOfFiles);
    	  CSVWriter writer = new CSVWriter(new FileWriter(Consts.OUTPUT_FILE));
    	      	  
    	  while(!queue.isEmpty()) {
    		  CSVReader topReader = queue.poll();
    		  String[] record = topReader.readNext();
    		  if(record != null) {
    			  writer.writeNext(record);
    			  if(topReader.peek() != null) {
    				  queue.add(topReader);  
    			  } else {
    				  logger.trace("Closing empty reader.");
    				  topReader.close();
    			  }
    		  }
    	  }
    	  logger.trace("Queue of readers is empty, all partial files were processed");
    	  writer.close();
      }
      
      /**
       * Creating priority queue with CSV reader for each partial file.
       * The priority of the queue is defined by the next line to read.
       * @param numberOfFiles - the number of partial files.
       * @return - a priority queue with CSVReaders.
       * @throws URISyntaxException
       * @throws IOException
       */
      private static PriorityQueue<CSVReader> loadReadersToPriorityQueue(int numberOfFiles) throws URISyntaxException, IOException {
    	  
    	  PriorityQueue<CSVReader> queue = new PriorityQueue<>(compareReaders);
    	  
    	  for(int i=1; i<=numberOfFiles; i++) {
    		  String fileName = "Sorted_part_" + i + ".csv";
    		  Path filePath = Paths.get(
    				  ClassLoader.getSystemResource(fileName).toURI()
    				  );
    		  Reader reader = Files.newBufferedReader(filePath);
    		  CSVReader csvReader = new CSVReader(reader);
    		queue.add(csvReader);
    	  }
    	  logger.trace("Created CSVReader priority queue of size: " + queue.size());
    	  return queue;
      }
     
     
}
