package com.example.dataintegration.connector;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FlatFileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(FlatFileProcessor.class);

    private String filePath;
    private String delimiter;

    public FlatFileProcessor(String filePath, String delimiter) {
        this.filePath = filePath;
        this.delimiter = delimiter;
    }

    public List<List<String>> readData() throws IOException {
        List<List<String>> data = new ArrayList<>();
        CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter.charAt(0)).withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim();
        try (CSVParser csvParser = CSVParser.parse(new File(filePath), java.nio.charset.StandardCharsets.UTF_8, csvFormat)) {
            for (CSVRecord csvRecord : csvParser) {
                List<String> row = new ArrayList<>();
                for (int i = 0; i < csvRecord.size(); i++) {
                    row.add(csvRecord.get(i));
                }
                data.add(row);
            }
        }
        return data;
    }

    public void writeData(List<List<String>> data) throws IOException {
        CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter.charAt(0));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

            for (List<String> row : data) {
                csvPrinter.printRecord(row);
            }
        }
    }

    public List<String> getFileSchema() throws IOException {
        List<String> schema = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine != null) {
                String[] headers = headerLine.split(delimiter);
                for (String header : headers) {
                    schema.add(header.trim());
                }
            }
        }
        return schema;
    }
}