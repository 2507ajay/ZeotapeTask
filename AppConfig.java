

import com.example.dataintegration.connector.ClickHouseConnector;
import com.example.dataintegration.connector.FlatFileProcessor;
import com.example.dataintegration.service.DataIngestionService;
import com.example.dataintegration.util.JsonUtil;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.util.Arrays;
import java.util.List;

public class AppConfig {

    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);

    public static void setupRoutes() {
        Spark.port(8080);
        Spark.staticFiles.externalLocation("src/main/resources/public"); // For serving HTML/CSS/JS

        //  Connect to ClickHouse or provide FlatFile details
        Spark.post("http://localhost:5000/connect", (req, res) -> {
            JSONObject requestBody = new JSONObject(req.body());
            String sourceType = requestBody.getString("sourceType");

            try {
                if ("ClickHouse".equalsIgnoreCase(sourceType)) {
                    String host = requestBody.getString("host");
                    int port = requestBody.getInt("port");
                    String database = requestBody.getString("database");
                    String user = requestBody.optString("user", ""); // Handle optional user
                    String jwtToken = requestBody.optString("jwtToken", null); // Handle optional JWT

                    ClickHouseConnector connector = new ClickHouseConnector(host, port, database, user, jwtToken);
                    connector.connect();
                    connector.close(); // Close immediately after testing connection
                    return JsonUtil.success("ClickHouse connection successful");

                } else if ("FlatFile".equalsIgnoreCase(sourceType)) {
                    String filePath = requestBody.getString("filePath");
                    String delimiter = requestBody.getString("delimiter");

                    //  Basic file existence check (can be enhanced)
                    java.io.File file = new java.io.File(filePath);
                    if (!file.exists() || file.isDirectory()) {
                        return JsonUtil.error("Invalid file path");
                    }

                    return JsonUtil.success("Flat File access successful");
                } else {
                    return JsonUtil.error("Invalid source type");
                }
            } catch (Exception e) {
                logger.error("Connection error: ", e);
                return JsonUtil.error("Connection failed: " + e.getMessage());
            }
        });

        //  Load tables (ClickHouse) or schema (FlatFile)
        Spark.post("http://localhost:5000/loadColumns", (req, res) -> {
            JSONObject requestBody = new JSONObject(req.body());
            String sourceType = requestBody.getString("sourceType");

            try {
                if ("ClickHouse".equalsIgnoreCase(sourceType)) {
                    String host = requestBody.getString("host");
                    int port = requestBody.getInt("port");
                    String database = requestBody.getString("database");
                    String user = requestBody.optString("user", "");
                    String jwtToken = requestBody.optString("jwtToken", null);
                    String tableName = requestBody.getString("tableName");

                    ClickHouseConnector connector = new ClickHouseConnector(host, port, database, user, jwtToken);
                    connector.connect();
                    List<String> columns = connector.getColumns(tableName);
                    connector.close();
                    return JsonUtil.data(columns);

                } else if ("FlatFile".equalsIgnoreCase(sourceType)) {
                    String filePath = requestBody.getString("filePath");
                    String delimiter = requestBody.getString("delimiter");

                    FlatFileProcessor processor = new FlatFileProcessor(filePath, delimiter);
                    List<String> schema = processor.getFileSchema();
                    return JsonUtil.data(schema);
                } else {
                    return JsonUtil.error("Invalid source type");
                }
            } catch (Exception e) {
                logger.error("Error loading columns/tables: ", e);
                return JsonUtil.error("Error loading columns/tables: " + e.getMessage());
            }
        });

        //  Initiate data ingestion
        Spark.post("http://localhost:5000/ingest", (req, res) -> {
            JSONObject requestBody = new JSONObject(req.body());
            String sourceType = requestBody.getString("sourceType");
            String targetType = requestBody.getString("targetType");

            try {
                if ("ClickHouse".equalsIgnoreCase(sourceType) && "FlatFile".equalsIgnoreCase(targetType)) {
                    // ClickHouse to FlatFile
                    String host = requestBody.getString("host");
                    int port = requestBody.getInt("port");
                    String database = requestBody.getString("database");
                    String user = requestBody.optString("user", "");
                    String jwtToken = requestBody.optString("jwtToken", null);
                    String tableName = requestBody.getString("tableName");
                    List<String> selectedColumns = Arrays.asList(requestBody.getString("selectedColumns").split(","));
                    String filePath = requestBody.getString("filePath");
                    String delimiter = requestBody.getString("delimiter");

                    ClickHouseConnector source = new ClickHouseConnector(host, port, database, user, jwtToken);
                    FlatFileProcessor target = new FlatFileProcessor(filePath, delimiter);
                    DataIngestionService ingestionService = new DataIngestionService();

                    long recordCount = ingestionService.clickHouseToFlatFile(source, target, tableName, selectedColumns);
                    return JsonUtil.success("Ingestion successful. Records processed: " + recordCount);

                } else if ("FlatFile".equalsIgnoreCase(sourceType) && "ClickHouse".equalsIgnoreCase(targetType)) {
                    // FlatFile to ClickHouse
                    String filePath = requestBody.getString("filePath");
                    String delimiter = requestBody.getString("delimiter");
                    String host = requestBody.getString("host");
                    int port = requestBody.getInt("port");
                    String database = requestBody.getString("database");
                    String user = requestBody.optString("user", "");
                    String jwtToken = requestBody.optString("jwtToken", null);
                    String targetTable = requestBody.getString("targetTable");
                    List<String> selectedColumns = Arrays.asList(requestBody.getString("selectedColumns").split(","));

                    FlatFileProcessor source = new FlatFileProcessor(filePath, delimiter);
                    ClickHouseConnector target = new ClickHouseConnector(host, port, database, user, jwtToken);
                    DataIngestionService ingestionService = new DataIngestionService();

                    long recordCount = ingestionService.flatFileToClickHouse(source, target, targetTable, selectedColumns);
                    return JsonUtil.success("Ingestion successful. Records inserted: " + recordCount);

                } else {
                    return JsonUtil.error("Invalid source or target type combination");
                }
            } catch (Exception e) {
                logger.error("Ingestion error: ", e);
                return JsonUtil.error("Ingestion failed: " + e.getMessage());
            }
        });

        Spark.exception(Exception.class, (e, req, res) -> {
            logger.error("Unhandled exception", e);
            res.status(500);
            res.body(JsonUtil.error("Internal Server Error: " + e.getMessage()));
        });
    }
}