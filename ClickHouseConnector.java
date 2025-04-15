package com.example.dataintegration.connector;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ClickHouseConnector {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseConnector.class);

    private String host;
    private int port;
    private String database;
    private String user;
    private String jwtToken;
    private Connection connection;

    public ClickHouseConnector(String host, int port, String database, String user, String jwtToken) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.jwtToken = jwtToken;
    }

    public void connect() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        if (jwtToken != null && !jwtToken.isEmpty()) {
            properties.setProperty("password", jwtToken); // Or however your driver handles JWT
        }
        String url = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(url, properties);

        logger.info("ClickHouse Connected to {}:{}", host, port);
    }

    public List<String> getTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    public List<String> getColumns(String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?")) {
            stmt.setString(1, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name"));
                }
            }
        }
        return columns;
    }

    public JSONArray executeQuery(String query) throws SQLException {
        JSONArray jsonArray = new JSONArray();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    jsonObject.put(columnName, columnValue);
                }
                jsonArray.put(jsonObject);
            }
        }
        return jsonArray;
    }

    public void executeInsert(String table, List<String> columns, List<List<Object>> values) throws SQLException {
        if (columns == null || columns.isEmpty() || values == null || values.isEmpty()) {
            throw new IllegalArgumentException("Invalid input for insert.");
        }

        StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i));
            if (i < columns.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(") VALUES  ");

        for (int i = 0; i < values.size(); i++) {
            List<Object> row = values.get(i);
            sb.append("(");
            for (int j = 0; j < row.size(); j++) {
                sb.append(convertValueToSQL(row.get(j)));
                if (j < row.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            if (i < values.size() - 1) {
                sb.append(", ");
            }
        }

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sb.toString());
        }
    }

    private String convertValueToSQL(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'"; // Escape single quotes
        } else {
            return value.toString();
        }
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
            logger.info("ClickHouse Connection Closed");
        }
    }
}