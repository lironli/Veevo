package com.example.veevo;

import java.io.IOException;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.opencsv.exceptions.CsvValidationException;

@SpringBootApplication
public class VeevoApplication {
	
	private static final Logger logger = LoggerFactory.getLogger(VeevoApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(VeevoApplication.class, args);

		logger.trace("Started external sorting");
		logger.info("Input file for current run: " + Consts.INPUT_FILE);
		
		try {
			int numOfSubFiles = ReadWriteFunctions.processFileAsBlocks();
			SortAndMergeFunctions.merge(numOfSubFiles);
			
			logger.trace("External sorting compleated.");
			
		} catch (CsvValidationException | IOException | URISyntaxException e) {
			logger.error("Something went wrong:");
			logger.error(e.toString());
		}
		
	}

}
