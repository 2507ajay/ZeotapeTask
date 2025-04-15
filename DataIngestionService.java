package com.example.dataintegration.service;

import com.example.dataintegration.connector.ClickHouseConnector;
import com.example.dataintegration.connector.FlatFileProcessor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);

    public long clickHouseToFlatFile(ClickHouseConnector source, FlatFileProcessor target, String tableName, List<String> selectedColumns) throws SQLException, IOException {
        StringBuilder queryBuilder = new StringBuilder("SELECT ");
        for (int i = 0; i < selectedColumns.size(); i++) {
            queryBuilder.append(selectedColumns.get(i));
            if (i < selectedColumns.size() - 1) {
                queryBuilder.append(", ");
            }
        }
        queryBuilder.append(" FROM ").append(tableName);

        source.connect();
        JSONArray results = source.executeQuery(queryBuilder.toString());
        source.close();

        List<List<String>> dataToWrite = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            JSONObject row = results.getJSONObject(i);
            List<String> rowData = new ArrayList<>();
            for (String col : selectedColumns) {
                rowData.add(row.optString(col)); // Use optString to handle potential nulls
            }
            dataToWrite.add(rowData);
        }

        target.writeData(dataToWrite);
        return dataToWrite.size();
    }

    public long flatFileToClickHouse(FlatFileProcessor source, ClickHouseConnector target, String targetTable, List<String> selectedColumns) throws IOException, SQLException {
        List<List<String>> data = source.readData();
        if (data.isEmpty()) {
            return 0; // Or throw an exception if empty file is an error
        }

        target.connect();
        List<String> targetColumns = new ArrayList<>(selectedColumns); // Reuse selected columns
        List<List<Object>> values = new ArrayList<>();
    }
}