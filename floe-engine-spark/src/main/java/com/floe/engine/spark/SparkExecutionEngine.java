package com.floe.engine.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.*;
import com.floe.core.exception.FloeEngineException;
import com.floe.core.maintenance.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution engine that submits Iceberg maintenance Spark jobs via Apache Livy.
 *
 * <p>floe-spark-job is the companion Spark job that performs the actual maintenance operations.
 *
 * <p>Uses Livy's batch API to submit spark-submit jobs.
 */
public class SparkExecutionEngine implements ExecutionEngine {

    private static final Logger LOG = LoggerFactory.getLogger(SparkExecutionEngine.class);

    private final SparkEngineConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, ExecutionTracker> activeExecutions;
    private final AtomicBoolean shutdown;

    public SparkExecutionEngine(SparkEngineConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jdk8.Jdk8Module());
        this.objectMapper.registerModule(
                new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        this.activeExecutions = new ConcurrentHashMap<>();
        this.shutdown = new AtomicBoolean(false);

        LOG.info(
                "SparkExecutionEngine initialized: url={}, jar={}",
                config.livyUrl(),
                config.maintenanceJobJar());
    }

    @Override
    public String getEngineName() {
        return "Spark Execution Engine";
    }

    @Override
    public EngineType getEngineType() {
        return EngineType.SPARK; // Livy runs Spark under the hood
    }

    @Override
    public boolean isOperational() {
        if (shutdown.get()) {
            return false;
        }
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(config.livyUrl() + "/batches"))
                            .GET()
                            .timeout(Duration.ofSeconds(10))
                            .build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            LOG.warn("Livy health check failed", e);
            return false;
        }
    }

    @Override
    public EngineCapabilities getCapabilities() {
        return EngineCapabilities.spark();
    }

    @Override
    public CompletableFuture<ExecutionResult> execute(
            TableIdentifier table, MaintenanceOperation operation, ExecutionContext context) {
        if (shutdown.get()) {
            return CompletableFuture.completedFuture(
                    ExecutionResult.failure(
                            context.executionId(),
                            table,
                            operation.getType(),
                            Instant.now(),
                            Instant.now(),
                            "Engine is shutdown",
                            ""));
        }

        String executionId = context.executionId();
        LOG.info("Submitting execution {}: {} on {}", executionId, operation.getType(), table);

        ExecutionTracker tracker = new ExecutionTracker(executionId, table, operation.getType());
        activeExecutions.put(executionId, tracker);

        CompletableFuture<ExecutionResult> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            Instant startTime = Instant.now();
                            tracker.setStatus(ExecutionStatus.RUNNING);

                            try {
                                // Submit batch job to Livy
                                int batchId = submitBatch(table, operation, context);
                                tracker.setBatchId(batchId);

                                LOG.info(
                                        "Submitted Livy batch {} for execution {}",
                                        batchId,
                                        executionId);

                                // Poll for completion
                                BatchResult result =
                                        pollForCompletion(batchId, context.timeoutSeconds());

                                Instant endTime = Instant.now();

                                if (result.success()) {
                                    tracker.setStatus(ExecutionStatus.SUCCEEDED);
                                    LOG.info("Execution {} completed successfully", executionId);
                                    return ExecutionResult.success(
                                            executionId,
                                            table,
                                            operation.getType(),
                                            startTime,
                                            endTime,
                                            result.metrics());
                                } else {
                                    tracker.setStatus(ExecutionStatus.FAILED);
                                    LOG.error(
                                            "Execution {} failed: {}",
                                            executionId,
                                            result.errorMessage());
                                    return ExecutionResult.failure(
                                            executionId,
                                            table,
                                            operation.getType(),
                                            startTime,
                                            endTime,
                                            result.errorMessage(),
                                            result.logs());
                                }
                            } catch (Exception e) {
                                Instant endTime = Instant.now();
                                tracker.setStatus(ExecutionStatus.FAILED);
                                String errorMessage =
                                        e.getMessage() != null ? e.getMessage() : "Unknown error";
                                String stackTrace = getStackTrace(e);
                                LOG.error("Execution {} failed: {}", executionId, errorMessage, e);
                                return ExecutionResult.failure(
                                        executionId,
                                        table,
                                        operation.getType(),
                                        startTime,
                                        endTime,
                                        errorMessage,
                                        stackTrace);
                            } finally {
                                activeExecutions.remove(executionId);
                            }
                        });

        tracker.setFuture(future);
        return future;
    }

    @Override
    public Optional<ExecutionStatus> getStatus(String executionId) {
        ExecutionTracker tracker = activeExecutions.get(executionId);
        if (tracker == null) {
            return Optional.empty();
        }
        return Optional.of(tracker.getStatus());
    }

    @Override
    public boolean cancelExecution(String executionId) {
        ExecutionTracker tracker = activeExecutions.get(executionId);
        if (tracker == null) {
            return false;
        }

        // Cancel the future
        if (tracker.getFuture() != null) {
            tracker.getFuture().cancel(true);
        }

        // Kill the Livy batch if it exists
        if (tracker.getBatchId() != null) {
            killBatch(tracker.getBatchId());
        }

        tracker.setStatus(ExecutionStatus.CANCELLED);
        activeExecutions.remove(executionId);
        LOG.info("Execution {} cancelled", executionId);
        return true;
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            LOG.info("Shutting down SparkExecutionEngine...");

            // Cancel all active executions and kill their batches
            for (ExecutionTracker tracker : activeExecutions.values()) {
                if (tracker.getFuture() != null) {
                    tracker.getFuture().cancel(true);
                }
                if (tracker.getBatchId() != null) {
                    killBatch(tracker.getBatchId());
                }
            }
            activeExecutions.clear();

            LOG.info("SparkExecutionEngine shutdown complete.");
        }
    }

    // Livy Batch Operations

    private int submitBatch(
            TableIdentifier table, MaintenanceOperation operation, ExecutionContext context)
            throws Exception {
        ObjectNode requestBody = objectMapper.createObjectNode();

        // Main class and jar
        requestBody.put("className", config.maintenanceJobClass());
        requestBody.put("file", config.maintenanceJobJar());

        // Arguments: operation type, catalog, namespace, table, config json
        ArrayNode args = requestBody.putArray("args");
        args.add(operation.getType().name());
        args.add(table.catalog());
        args.add(table.namespace());
        args.add(table.getTableName());
        args.add(serializeOperationConfig(operation));
        args.add(context.executionId());

        // Spark configuration
        ObjectNode conf = requestBody.putObject("conf");

        // Iceberg extensions (include Nessie extensions if using Nessie)
        String extensions = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
        if (config.catalogProperties().containsKey("catalog-impl")
                && config.catalogProperties().get("catalog-impl").contains("NessieCatalog")) {
            extensions += ",org.projectnessie.spark.extensions.NessieSparkSessionExtensions";
        }
        conf.put("spark.sql.extensions", extensions);

        // Catalog configuration
        String catalogName = table.catalog();
        conf.put("spark.sql.defaultCatalog", catalogName);
        conf.put("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog");

        for (Map.Entry<String, String> entry : config.catalogProperties().entrySet()) {
            conf.put("spark.sql.catalog." + catalogName + "." + entry.getKey(), entry.getValue());
        }

        // S3/Hadoop configuration
        for (Map.Entry<String, String> entry : config.sparkConf().entrySet()) {
            conf.put(entry.getKey(), entry.getValue());
        }

        // Driver/executor resources
        if (config.driverMemory() != null) {
            conf.put("spark.driver.memory", config.driverMemory());
        }
        if (config.executorMemory() != null) {
            conf.put("spark.executor.memory", config.executorMemory());
        }

        String json = objectMapper.writeValueAsString(requestBody);
        LOG.debug("Submitting Livy batch: {}", json);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(config.livyUrl() + "/batches"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .timeout(Duration.ofSeconds(30))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 201 && response.statusCode() != 200) {
            throw new FloeEngineException(
                    "Spark",
                    "submit batch",
                    context.executionId(),
                    "Failed to submit batch (status "
                            + response.statusCode()
                            + "): "
                            + response.body());
        }

        JsonNode responseJson = objectMapper.readTree(response.body());
        return responseJson.get("id").asInt();
    }

    private BatchResult pollForCompletion(int batchId, int timeoutSeconds) throws Exception {
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeoutSeconds * 1000L;

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(config.livyUrl() + "/batches/" + batchId))
                            .GET()
                            .timeout(Duration.ofSeconds(10))
                            .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode status = objectMapper.readTree(response.body());

            String state = status.get("state").asText();
            LOG.debug("Batch {} state: {}", batchId, state);

            switch (state) {
                case "success":
                    String logs = fetchBatchLogs(batchId);
                    Map<String, Object> metrics = extractMetricsFromLogs(logs);
                    metrics.put("batchId", batchId);
                    return new BatchResult(true, metrics, null, logs);
                case "dead":
                case "error":
                case "killed":
                    String failureLogs = fetchBatchLogs(batchId);
                    return new BatchResult(false, Map.of(), "Batch " + state, failureLogs);
                case "running":
                case "starting":
                    Thread.sleep(config.pollIntervalMs());
                    break;
                default:
                    LOG.warn("Unknown batch state: {}", state);
                    Thread.sleep(config.pollIntervalMs());
            }
        }

        // Timeout - kill the batch
        killBatch(batchId);
        return new BatchResult(
                false, Map.of(), "Batch timed out after " + timeoutSeconds + " seconds", "");
    }

    private String fetchBatchLogs(int batchId) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(config.livyUrl() + "/batches/" + batchId + "/log"))
                            .GET()
                            .timeout(Duration.ofSeconds(10))
                            .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode logsJson = objectMapper.readTree(response.body());

            if (logsJson.has("log")) {
                StringBuilder logs = new StringBuilder();
                for (JsonNode line : logsJson.get("log")) {
                    logs.append(line.asText()).append("\n");
                }
                return logs.toString();
            }
        } catch (Exception e) {
            LOG.warn("Failed to fetch batch logs for batch {}", batchId, e);
        }
        return "";
    }

    private void killBatch(int batchId) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(config.livyUrl() + "/batches/" + batchId))
                            .DELETE()
                            .timeout(Duration.ofSeconds(10))
                            .build();

            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            LOG.info("Killed Livy batch {}", batchId);
        } catch (Exception e) {
            LOG.warn("Failed to kill batch {}", batchId, e);
        }
    }

    private String serializeOperationConfig(MaintenanceOperation operation) throws Exception {
        return objectMapper.writeValueAsString(operation);
    }

    Map<String, Object> extractMetricsFromLogs(String logs) {
        if (logs == null || logs.isBlank()) {
            return new HashMap<>();
        }
        Map<String, Object> metrics = new HashMap<>();
        String[] lines = logs.split("\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                JsonNode node = objectMapper.readTree(trimmed);
                if (node.has("metricsType")
                        && "floe".equalsIgnoreCase(node.get("metricsType").asText())
                        && node.has("metrics")
                        && node.get("metrics").isObject()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> parsed =
                            objectMapper.convertValue(node.get("metrics"), Map.class);
                    metrics.putAll(parsed);
                }
            } catch (Exception e) {
                // Ignore non-JSON log lines
            }
        }
        return metrics;
    }

    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private record BatchResult(
            boolean success, Map<String, Object> metrics, String errorMessage, String logs) {}

    private static class ExecutionTracker {

        private final String executionId;
        private final TableIdentifier table;
        private final MaintenanceOperation.Type operationType;
        private volatile ExecutionStatus status;
        private volatile CompletableFuture<ExecutionResult> future;
        private volatile Integer batchId;

        ExecutionTracker(
                String executionId,
                TableIdentifier table,
                MaintenanceOperation.Type operationType) {
            this.executionId = executionId;
            this.table = table;
            this.operationType = operationType;
            this.status = ExecutionStatus.PENDING;
        }

        ExecutionStatus getStatus() {
            return status;
        }

        void setStatus(ExecutionStatus status) {
            this.status = status;
        }

        CompletableFuture<ExecutionResult> getFuture() {
            return future;
        }

        void setFuture(CompletableFuture<ExecutionResult> future) {
            this.future = future;
        }

        Integer getBatchId() {
            return batchId;
        }

        void setBatchId(Integer batchId) {
            this.batchId = batchId;
        }

        @Override
        public String toString() {
            return String.format(
                    "ExecutionTracker{executionId='%s', table=%s, operationType=%s, status=%s, batchId=%s}",
                    executionId, table, operationType, status, batchId);
        }
    }
}
