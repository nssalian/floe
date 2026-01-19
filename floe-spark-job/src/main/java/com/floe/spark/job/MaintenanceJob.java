package com.floe.spark.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serial;
import java.util.Locale;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;

/**
 * Spark application for running Iceberg maintenance operations.
 *
 * <p>This is submitted via Livy as a batch job. One operation is performed per job execution.
 *
 * <p>Arguments: 0: Operation type (REWRITE_DATA_FILES, EXPIRE_SNAPSHOTS, etc.) 1: Catalog name 2:
 * Namespace 3: Table name 4: Operation config as JSON
 */
public final class MaintenanceJob {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MaintenanceJob.class);
    static final int EXPECTED_ARG_COUNT = 5;

    private MaintenanceJob() {}

    /**
     * Parses and validates command line arguments.
     *
     * @param args command line arguments
     * @return parsed JobArgs or null if invalid
     */
    static JobArgs parseArgs(String[] args) {
        if (args == null || args.length < EXPECTED_ARG_COUNT) {
            return null;
        }
        return new JobArgs(args[0], args[1], args[2], args[3], args[4]);
    }

    /**
     * Validates the operation type.
     *
     * @param operationType the operation to validate
     * @return true if valid
     */
    static boolean isValidOperationType(String operationType) {
        if (operationType == null) {
            return false;
        }
        return switch (operationType) {
            case "REWRITE_DATA_FILES", "EXPIRE_SNAPSHOTS", "REWRITE_MANIFESTS", "ORPHAN_CLEANUP" ->
                    true;
            default -> false;
        };
    }

    /** Holds parsed job arguments. */
    record JobArgs(
            String operationType,
            String catalogName,
            String namespace,
            String tableName,
            String configJson) {
        /** Returns the full table name in {namespace.table} format. */
        String fullTableName() {
            return namespace + "." + tableName;
        }
    }

    public static void main(String[] args) {
        validateArgs(args);

        String operationType = args[0];
        String catalogName = args[1];
        String namespace = args[2];
        String tableName = args[3];
        String configJson = args[4];

        String fullTableName = namespace + "." + tableName;

        logJobHeader(operationType, fullTableName, configJson);

        try (SparkSession spark = createSparkSession(operationType, tableName)) {
            runMaintenanceJob(spark, catalogName, fullTableName, operationType, configJson);
            logJobCompletion();
        }
    }

    private static void validateArgs(String[] args) {
        if (args.length < EXPECTED_ARG_COUNT) {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Invalid arguments. Expected 5 arguments: "
                                + "<operation> <catalog> <namespace> <table> <configJson>");
            }
            System.exit(1);
        }
    }

    private static void runMaintenanceJob(
            SparkSession spark,
            String catalogName,
            String fullTableName,
            String operationType,
            String configJson) {
        logCatalogInfo(spark, catalogName);

        Table table = loadTable(spark, fullTableName);
        JsonNode config = parseConfig(configJson);

        executeOperation(spark, table, operationType, config);
    }

    private static Table loadTable(SparkSession spark, String fullTableName) {
        try {
            return Spark3Util.loadIcebergTable(spark, fullTableName);
        } catch (AnalysisException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Failed to load table: {}", e.getMessage(), e);
            }
            throw new MaintenanceJobException("Failed to load table", e);
        }
    }

    /**
     * Parses the config JSON string into a JsonNode.
     *
     * @param configJson the JSON configuration string
     * @return parsed JsonNode
     * @throws MaintenanceJobException if parsing fails
     */
    static JsonNode parseConfig(String configJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(configJson);
        } catch (JsonProcessingException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Failed to parse config JSON: {}", e.getMessage(), e);
            }
            throw new MaintenanceJobException("Failed to parse config JSON", e);
        }
    }

    private static void logJobHeader(
            String operationType, String fullTableName, String configJson) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Floe Maintenance Job");
            LOG.info("Operation: {}", operationType);
            LOG.info("Table: {}", fullTableName);
            LOG.info("Config: {}", configJson);
        }
    }

    private static SparkSession createSparkSession(String operationType, String tableName) {
        SparkSession.Builder builder = SparkSession.builder();
        String appName = "Floe-" + operationType + "-" + tableName;
        builder.appName(appName);
        return builder.getOrCreate();
    }

    private static void logCatalogInfo(SparkSession spark, String catalogName) {
        if (LOG.isInfoEnabled()) {
            String catalogValue = spark.conf().get("spark.sql.catalog." + catalogName, "<NOT SET>");
            LOG.info("Catalog {} registered as: {}", catalogName, catalogValue);
        }
    }

    private static void executeOperation(
            SparkSession spark, Table table, String operationType, JsonNode config) {
        switch (operationType) {
            case "REWRITE_DATA_FILES" -> runRewriteDataFiles(spark, table, config);
            case "EXPIRE_SNAPSHOTS" -> runExpireSnapshots(spark, table, config);
            case "REWRITE_MANIFESTS" -> runRewriteManifests(spark, table, config);
            case "ORPHAN_CLEANUP" -> runOrphanCleanup(spark, table, config);
            default -> throw new IllegalArgumentException("Unknown operation: " + operationType);
        }
    }

    private static void logJobCompletion() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Floe Maintenance Job Completed Successfully");
        }
    }

    private static void runRewriteDataFiles(SparkSession spark, Table table, JsonNode config) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Running REWRITE_DATA_FILES (compaction)...");
        }

        SparkActions sparkActions = SparkActions.get(spark);
        RewriteDataFiles action = sparkActions.rewriteDataFiles(table);

        configureRewriteDataFiles(action, config);

        RewriteDataFiles.Result result = action.execute();

        logRewriteDataFilesResult(result);
    }

    private static void configureRewriteDataFiles(RewriteDataFiles action, JsonNode config) {
        // Strategy handling (method calls, not options)
        String strategy = config.has("strategy") ? config.get("strategy").asText() : "BINPACK";
        switch (strategy.toUpperCase(Locale.ROOT)) {
            case "SORT" -> {
                if (config.has("sortOrder") && config.get("sortOrder").isArray()) {
                    // For now, use default sort order - custom sort order requires SortOrder
                    // building
                    action.sort();
                } else {
                    action.sort();
                }
            }
            case "ZORDER" -> {
                if (config.has("zOrderColumns") && config.get("zOrderColumns").isArray()) {
                    JsonNode cols = config.get("zOrderColumns");
                    String[] columns = new String[cols.size()];
                    for (int i = 0; i < cols.size(); i++) {
                        columns[i] = cols.get(i).asText();
                    }
                    action.zOrder(columns);
                }
            }
            default -> action.binPack();
        }

        // Apply options
        applyOptionIfPresent(action, config, "targetFileSizeBytes", "target-file-size-bytes");
        applyOptionIfPresent(action, config, "maxFileGroupSizeBytes", "max-file-group-size-bytes");
        applyOptionIfPresent(
                action,
                config,
                "maxConcurrentFileGroupRewrites",
                "max-concurrent-file-group-rewrites");
        applyOptionIfPresent(action, config, "partialProgressEnabled", "partial-progress.enabled");
        applyOptionIfPresent(
                action, config, "partialProgressMaxCommits", "partial-progress.max-commits");
        applyOptionIfPresent(
                action,
                config,
                "partialProgressMaxFailedCommits",
                "partial-progress.max-failed-commits");
        applyOptionIfPresent(action, config, "rewriteJobOrder", "rewrite-job-order");
        applyOptionIfPresent(
                action, config, "useStartingSequenceNumber", "use-starting-sequence-number");
        applyOptionIfPresent(action, config, "removeDanglingDeletes", "remove-dangling-deletes");
        applyOptionIfPresent(action, config, "outputSpecId", "output-spec-id");

        // Apply filter expression (expects Iceberg JSON expression format)
        if (config.has("filter")) {
            JsonNode filterNode = config.get("filter");
            if (!filterNode.isNull() && !filterNode.asText().isBlank()) {
                action.filter(ExpressionParser.fromJson(filterNode.asText()));
            }
        }
    }

    private static void applyOptionIfPresent(
            RewriteDataFiles action, JsonNode config, String jsonField, String optionName) {
        if (config.has(jsonField)) {
            JsonNode value = config.get(jsonField);
            if (!value.isNull()) {
                action.option(optionName, value.asText());
            }
        }
    }

    private static void logRewriteDataFilesResult(RewriteDataFiles.Result result) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Compaction results:");
            LOG.info("  - Rewritten files: {}", result.rewrittenDataFilesCount());
            LOG.info("  - Added files: {}", result.addedDataFilesCount());
            LOG.info("  - Rewritten bytes: {}", result.rewrittenBytesCount());
        }
    }

    private static void runExpireSnapshots(SparkSession spark, Table table, JsonNode config) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Running EXPIRE_SNAPSHOTS...");
        }

        SparkActions sparkActions = SparkActions.get(spark);
        ExpireSnapshots action = sparkActions.expireSnapshots(table);

        configureExpireSnapshots(action, config);

        ExpireSnapshots.Result result = action.execute();

        logExpireSnapshotsResult(result);
    }

    private static void configureExpireSnapshots(ExpireSnapshots action, JsonNode config) {
        if (config.has("retainLast")) {
            JsonNode retainLastNode = config.get("retainLast");
            if (!retainLastNode.isNull()) {
                action.retainLast(retainLastNode.asInt());
            }
        }
        // Duration is serialized as seconds (decimal) by Jackson JavaTimeModule
        if (config.has("maxSnapshotAge")) {
            JsonNode maxAgeNode = config.get("maxSnapshotAge");
            if (!maxAgeNode.isNull()) {
                long ageMs = (long) (maxAgeNode.asDouble() * 1000);
                action.expireOlderThan(System.currentTimeMillis() - ageMs);
            }
        }
        if (config.has("cleanExpiredMetadata")) {
            JsonNode cleanMetadataNode = config.get("cleanExpiredMetadata");
            if (!cleanMetadataNode.isNull()) {
                action.cleanExpiredMetadata(cleanMetadataNode.asBoolean());
            }
        }
        if (config.has("expireSnapshotId")) {
            JsonNode expireSnapshotIdNode = config.get("expireSnapshotId");
            if (!expireSnapshotIdNode.isNull()) {
                action.expireSnapshotId(expireSnapshotIdNode.asLong());
            }
        }
    }

    private static void logExpireSnapshotsResult(ExpireSnapshots.Result result) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Expire snapshots results:");
            LOG.info("  - Deleted data files: {}", result.deletedDataFilesCount());
            LOG.info("  - Deleted manifests: {}", result.deletedManifestsCount());
            LOG.info("  - Deleted manifest lists: {}", result.deletedManifestListsCount());
        }
    }

    private static void runRewriteManifests(SparkSession spark, Table table, JsonNode config) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Running REWRITE_MANIFESTS...");
        }

        SparkActions sparkActions = SparkActions.get(spark);
        var action = sparkActions.rewriteManifests(table);

        configureRewriteManifests(action, config);

        var result = action.execute();

        logRewriteManifestsResult(result);
    }

    private static void configureRewriteManifests(
            org.apache.iceberg.actions.RewriteManifests action, JsonNode config) {
        if (config.has("specId")) {
            JsonNode specIdNode = config.get("specId");
            if (!specIdNode.isNull()) {
                action.specId(specIdNode.asInt());
            }
        }
        if (config.has("stagingLocation")) {
            JsonNode stagingNode = config.get("stagingLocation");
            if (!stagingNode.isNull()) {
                action.stagingLocation(stagingNode.asText());
            }
        }
        if (config.has("sortBy")) {
            JsonNode sortByNode = config.get("sortBy");
            if (!sortByNode.isNull() && sortByNode.isArray()) {
                java.util.List<String> fields = new java.util.ArrayList<>();
                for (JsonNode field : sortByNode) {
                    fields.add(field.asText());
                }
                if (!fields.isEmpty()) {
                    action.sortBy(fields);
                }
            }
        }
        // Build predicate from rewriteIf
        if (config.has("rewriteIf")) {
            JsonNode filterNode = config.get("rewriteIf");
            if (!filterNode.isNull()) {
                action.rewriteIf(buildManifestPredicate(filterNode));
            }
        }
    }

    private static java.util.function.Predicate<ManifestFile> buildManifestPredicate(
            JsonNode filter) {
        return manifest -> {
            // PATH
            if (filter.has("path")) {
                JsonNode node = filter.get("path");
                if (!node.isNull() && !manifest.path().equals(node.asText())) {
                    return false;
                }
            }
            // LENGTH
            if (filter.has("length")) {
                JsonNode node = filter.get("length");
                if (!node.isNull() && manifest.length() != node.asLong()) {
                    return false;
                }
            }
            // SPEC_ID
            if (filter.has("specId")) {
                JsonNode node = filter.get("specId");
                if (!node.isNull() && manifest.partitionSpecId() != node.asInt()) {
                    return false;
                }
            }
            // MANIFEST_CONTENT (DATA or DELETES)
            if (filter.has("content")) {
                JsonNode node = filter.get("content");
                if (!node.isNull()) {
                    String expected = node.asText().toUpperCase(Locale.ROOT);
                    if (!manifest.content().name().equals(expected)) {
                        return false;
                    }
                }
            }
            // SEQUENCE_NUMBER
            if (filter.has("sequenceNumber")) {
                JsonNode node = filter.get("sequenceNumber");
                if (!node.isNull() && manifest.sequenceNumber() != node.asLong()) {
                    return false;
                }
            }
            // MIN_SEQUENCE_NUMBER
            if (filter.has("minSequenceNumber")) {
                JsonNode node = filter.get("minSequenceNumber");
                if (!node.isNull() && manifest.minSequenceNumber() != node.asLong()) {
                    return false;
                }
            }
            // SNAPSHOT_ID
            if (filter.has("snapshotId")) {
                JsonNode node = filter.get("snapshotId");
                if (!node.isNull() && manifest.snapshotId() != node.asLong()) {
                    return false;
                }
            }
            // ADDED_FILES_COUNT
            if (filter.has("addedFilesCount")) {
                JsonNode node = filter.get("addedFilesCount");
                if (!node.isNull()) {
                    Integer count = manifest.addedFilesCount();
                    if (count == null || count != node.asInt()) {
                        return false;
                    }
                }
            }
            // EXISTING_FILES_COUNT
            if (filter.has("existingFilesCount")) {
                JsonNode node = filter.get("existingFilesCount");
                if (!node.isNull()) {
                    Integer count = manifest.existingFilesCount();
                    if (count == null || count != node.asInt()) {
                        return false;
                    }
                }
            }
            // DELETED_FILES_COUNT
            if (filter.has("deletedFilesCount")) {
                JsonNode node = filter.get("deletedFilesCount");
                if (!node.isNull()) {
                    Integer count = manifest.deletedFilesCount();
                    if (count == null || count != node.asInt()) {
                        return false;
                    }
                }
            }
            // ADDED_ROWS_COUNT
            if (filter.has("addedRowsCount")) {
                JsonNode node = filter.get("addedRowsCount");
                if (!node.isNull()) {
                    Long count = manifest.addedRowsCount();
                    if (count == null || count != node.asLong()) {
                        return false;
                    }
                }
            }
            // EXISTING_ROWS_COUNT
            if (filter.has("existingRowsCount")) {
                JsonNode node = filter.get("existingRowsCount");
                if (!node.isNull()) {
                    Long count = manifest.existingRowsCount();
                    if (count == null || count != node.asLong()) {
                        return false;
                    }
                }
            }
            // DELETED_ROWS_COUNT
            if (filter.has("deletedRowsCount")) {
                JsonNode node = filter.get("deletedRowsCount");
                if (!node.isNull()) {
                    Long count = manifest.deletedRowsCount();
                    if (count == null || count != node.asLong()) {
                        return false;
                    }
                }
            }
            // FIRST_ROW_ID
            if (filter.has("firstRowId")) {
                JsonNode node = filter.get("firstRowId");
                if (!node.isNull()) {
                    Long firstRowId = manifest.firstRowId();
                    if (firstRowId == null || firstRowId != node.asLong()) {
                        return false;
                    }
                }
            }
            // KEY_METADATA - compare as hex string
            if (filter.has("keyMetadata")) {
                JsonNode node = filter.get("keyMetadata");
                if (!node.isNull()) {
                    java.nio.ByteBuffer keyMeta = manifest.keyMetadata();
                    if (keyMeta == null) {
                        return false;
                    }
                    String hexValue = bytesToHex(keyMeta);
                    if (!hexValue.equals(node.asText())) {
                        return false;
                    }
                }
            }
            // PARTITION_SUMMARIES - compare as list
            if (filter.has("partitionSummaries")) {
                JsonNode node = filter.get("partitionSummaries");
                if (!node.isNull() && node.isArray()) {
                    java.util.List<ManifestFile.PartitionFieldSummary> summaries =
                            manifest.partitions();
                    if (summaries == null || summaries.size() != node.size()) {
                        return false;
                    }
                    for (int i = 0; i < node.size(); i++) {
                        JsonNode summaryNode = node.get(i);
                        ManifestFile.PartitionFieldSummary summary = summaries.get(i);
                        if (!matchesPartitionSummary(summary, summaryNode)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        };
    }

    private static boolean matchesPartitionSummary(
            ManifestFile.PartitionFieldSummary summary, JsonNode node) {
        if (node.has("containsNull")) {
            if (summary.containsNull() != node.get("containsNull").asBoolean()) {
                return false;
            }
        }
        if (node.has("containsNan")) {
            JsonNode nanNode = node.get("containsNan");
            if (!nanNode.isNull()) {
                Boolean containsNan = summary.containsNaN();
                if (containsNan == null || containsNan != nanNode.asBoolean()) {
                    return false;
                }
            }
        }
        if (node.has("lowerBound")) {
            JsonNode lbNode = node.get("lowerBound");
            if (!lbNode.isNull()) {
                java.nio.ByteBuffer lb = summary.lowerBound();
                if (lb == null) {
                    return false;
                }
                if (!bytesToHex(lb).equals(lbNode.asText())) {
                    return false;
                }
            }
        }
        if (node.has("upperBound")) {
            JsonNode ubNode = node.get("upperBound");
            if (!ubNode.isNull()) {
                java.nio.ByteBuffer ub = summary.upperBound();
                if (ub == null) {
                    return false;
                }
                if (!bytesToHex(ub).equals(ubNode.asText())) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String bytesToHex(java.nio.ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static void logRewriteManifestsResult(
            org.apache.iceberg.actions.RewriteManifests.Result result) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Rewrite manifests results:");
            Iterable<ManifestFile> rewrittenManifests = result.rewrittenManifests();
            for (ManifestFile manifest : rewrittenManifests) {
                LOG.info("  - Rewritten Manifest: {}", manifest.path());
            }
            Iterable<ManifestFile> addedManifests = result.addedManifests();
            for (ManifestFile manifest : addedManifests) {
                LOG.info("  - Added Manifest: {}", manifest.path());
            }
        }
    }

    private static void runOrphanCleanup(SparkSession spark, Table table, JsonNode config) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Running ORPHAN_CLEANUP...");
        }

        SparkActions sparkActions = SparkActions.get(spark);
        var action = sparkActions.deleteOrphanFiles(table);

        configureOrphanCleanup(action, config);

        var result = action.execute();

        logOrphanCleanupResult(result);
    }

    private static void configureOrphanCleanup(
            org.apache.iceberg.actions.DeleteOrphanFiles action, JsonNode config) {
        // Duration is serialized as seconds (decimal) by Jackson JavaTimeModule
        if (config.has("olderThan")) {
            JsonNode olderThanNode = config.get("olderThan");
            if (!olderThanNode.isNull()) {
                long ageMs = (long) (olderThanNode.asDouble() * 1000);
                action.olderThan(System.currentTimeMillis() - ageMs);
            }
        }
        if (config.has("location")) {
            JsonNode locationNode = config.get("location");
            if (!locationNode.isNull()) {
                action.location(locationNode.asText());
            }
        }
        if (config.has("prefixMismatchMode")) {
            JsonNode modeNode = config.get("prefixMismatchMode");
            if (!modeNode.isNull()) {
                action.prefixMismatchMode(
                        org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode.valueOf(
                                modeNode.asText().toUpperCase(Locale.ROOT)));
            }
        }
        if (config.has("equalSchemes")) {
            JsonNode schemesNode = config.get("equalSchemes");
            if (!schemesNode.isNull() && schemesNode.isObject()) {
                java.util.Map<String, String> schemes = new java.util.HashMap<>();
                schemesNode
                        .fields()
                        .forEachRemaining(e -> schemes.put(e.getKey(), e.getValue().asText()));
                if (!schemes.isEmpty()) {
                    action.equalSchemes(schemes);
                }
            }
        }
        if (config.has("equalAuthorities")) {
            JsonNode authoritiesNode = config.get("equalAuthorities");
            if (!authoritiesNode.isNull() && authoritiesNode.isObject()) {
                java.util.Map<String, String> authorities = new java.util.HashMap<>();
                authoritiesNode
                        .fields()
                        .forEachRemaining(e -> authorities.put(e.getKey(), e.getValue().asText()));
                if (!authorities.isEmpty()) {
                    action.equalAuthorities(authorities);
                }
            }
        }
    }

    private static void logOrphanCleanupResult(
            org.apache.iceberg.actions.DeleteOrphanFiles.Result result) {
        if (LOG.isInfoEnabled()) {
            Iterable<String> orphanFiles = result.orphanFileLocations();
            LOG.info("Orphan cleanup results:");
            for (String filePath : orphanFiles) {
                LOG.info("  - Deleted Orphan File: {}", filePath);
            }
        }
    }

    public static class MaintenanceJobException extends RuntimeException {

        @Serial private static final long serialVersionUID = 1L;

        public MaintenanceJobException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
