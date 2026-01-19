package com.floe.core.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads maintenance policies from YAML files or directories. */
public class PolicyLoader {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyLoader.class);
    private final ObjectMapper yamlMapper;

    public PolicyLoader() {
        this.yamlMapper =
                new ObjectMapper(new YAMLFactory())
                        .registerModule(new JavaTimeModule())
                        .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
    }

    /** Load policies from a YAML file */
    public List<MaintenancePolicy> loadFromFile(Path path) throws IOException {
        LOG.info("Loading policies from file: {}", path);
        try (InputStream inputStream = Files.newInputStream(path)) {
            return loadFromStream(inputStream);
        }
    }

    /** Load policies from an input stream. */
    public List<MaintenancePolicy> loadFromStream(InputStream inputStream) throws IOException {
        PolicyFileModel fileModel = yamlMapper.readValue(inputStream, PolicyFileModel.class);
        return convertToPolices(fileModel);
    }

    /** Load policies and add them to a store. */
    public void loadIntoStore(Path source, PolicyStore store) throws IOException {
        List<MaintenancePolicy> policies;
        if (Files.isDirectory(source)) {
            policies = loadFromDirectory(source);
        } else {
            policies = loadFromFile(source);
        }
        store.saveAll(policies);
    }

    /** Load from directory */
    public List<MaintenancePolicy> loadFromDirectory(Path directory) throws IOException {
        LOG.info("Loading policies from directory: {}", directory);
        List<MaintenancePolicy> allPolicies = new ArrayList<>();

        try (Stream<Path> paths = Files.list(directory)) {
            List<Path> yamlFiles =
                    paths.filter(Files::isRegularFile)
                            .filter(
                                    p -> {
                                        String name =
                                                p.getFileName()
                                                        .toString()
                                                        .toLowerCase(java.util.Locale.ROOT);
                                        return name.endsWith(".yaml") || name.endsWith(".yml");
                                    })
                            .sorted()
                            .toList();

            for (Path yamlFile : yamlFiles) {
                try {
                    List<MaintenancePolicy> policies = loadFromFile(yamlFile);
                    allPolicies.addAll(policies);
                    LOG.debug(
                            "Loaded {} policies from {}", policies.size(), yamlFile.getFileName());
                } catch (IOException e) {
                    LOG.error("Failed to load policies from {}: {}", yamlFile, e.getMessage());
                    throw e;
                }
            }
        }

        LOG.info("Loaded {} total policies from directory", allPolicies.size());
        return allPolicies;
    }

    private List<MaintenancePolicy> convertToPolices(PolicyFileModel fileModel) {
        if (fileModel.policies == null) {
            return Collections.emptyList();
        }

        return fileModel.policies.stream().map(this::convertPolicy).toList();
    }

    /** Converts a PolicyModel from YAML into a MaintenancePolicy domain object. */
    private MaintenancePolicy convertPolicy(PolicyModel model) {
        MaintenancePolicy.Builder builder = MaintenancePolicy.builder();

        if (model.name != null) {
            builder.name(model.name);
        }
        if (model.description != null) {
            builder.description(model.description);
        }
        if (model.pattern != null) {
            builder.tablePattern(TablePattern.parse(model.pattern));
        }
        if (model.enabled != null) {
            builder.enabled(model.enabled);
        }

        if (model.priority != null) {
            builder.priority(model.priority);
        }
        if (model.tags != null) {
            builder.tags(model.tags);
        }

        builder.rewriteDataFiles(convertRewriteDataFiles(model.rewriteDataFiles));
        builder.rewriteDataFilesSchedule(convertSchedule(model.rewriteDataFilesSchedule));
        builder.expireSnapshots(convertExpireSnapshots(model.expireSnapshots));
        builder.expireSnapshotsSchedule(convertSchedule(model.expireSnapshotsSchedule));
        builder.orphanCleanup(convertOrphanCleanup(model.orphanCleanup));
        builder.orphanCleanupSchedule(convertSchedule(model.orphanCleanupSchedule));
        builder.rewriteManifests(convertRewriteManifests(model.rewriteManifests));
        builder.rewriteManifestsSchedule(convertSchedule(model.rewriteManifestsSchedule));

        if (model.createdAt != null) {
            builder.createdAt(model.createdAt);
        }
        if (model.updatedAt != null) {
            builder.updatedAt(model.updatedAt);
        }
        return builder.build();
    }

    private RewriteDataFilesConfig convertRewriteDataFiles(RewriteDataFilesModel model) {
        if (model == null) {
            return null;
        }
        RewriteDataFilesConfig.Builder builder = RewriteDataFilesConfig.builder();
        if (model.strategy != null) {
            builder.strategy(model.strategy.toUpperCase(java.util.Locale.ROOT));
        }
        if (model.sortOrder != null) {
            builder.sortOrder(model.sortOrder);
        }
        if (model.zOrderColumns != null) {
            builder.zOrderColumns(model.zOrderColumns);
        }
        if (model.targetFileSizeBytes != null) {
            builder.targetFileSizeBytes(model.targetFileSizeBytes);
        }
        if (model.maxFileGroupSizeBytes != null) {
            builder.maxFileGroupSizeBytes(model.maxFileGroupSizeBytes);
        }
        if (model.maxConcurrentFileGroupRewrites != null) {
            builder.maxConcurrentFileGroupRewrites(model.maxConcurrentFileGroupRewrites);
        }
        if (model.partialProgressEnabled != null) {
            builder.partialProgressEnabled(model.partialProgressEnabled);
        }
        if (model.partialProgressMaxCommits != null) {
            builder.partialProgressMaxCommits(model.partialProgressMaxCommits);
        }
        if (model.partialProgressMaxFailedCommits != null) {
            builder.partialProgressMaxFailedCommits(model.partialProgressMaxFailedCommits);
        }
        if (model.filter != null) {
            builder.filter(model.filter);
        }
        if (model.rewriteJobOrder != null) {
            builder.rewriteJobOrder(model.rewriteJobOrder);
        }
        if (model.useStartingSequenceNumber != null) {
            builder.useStartingSequenceNumber(model.useStartingSequenceNumber);
        }
        if (model.removeDanglingDeletes != null) {
            builder.removeDanglingDeletes(model.removeDanglingDeletes);
        }
        if (model.outputSpecId != null) {
            builder.outputSpecId(model.outputSpecId);
        }
        return builder.build();
    }

    private ExpireSnapshotsConfig convertExpireSnapshots(ExpireSnapshotsModel model) {
        if (model == null) {
            return null;
        }
        ExpireSnapshotsConfig.Builder builder = ExpireSnapshotsConfig.builder();
        if (model.retainLast != null) {
            builder.retainLast(model.retainLast);
        }
        if (model.maxSnapshotAgeDays != null) {
            builder.maxSnapshotAge(Duration.ofDays(model.maxSnapshotAgeDays));
        }
        if (model.cleanExpiredMetadata != null) {
            builder.cleanExpiredMetadata(model.cleanExpiredMetadata);
        }
        return builder.build();
    }

    private OrphanCleanupConfig convertOrphanCleanup(OrphanCleanupModel model) {
        if (model == null) {
            return null;
        }
        OrphanCleanupConfig.Builder builder = OrphanCleanupConfig.builder();
        if (model.retentionPeriodDays != null) {
            builder.retentionPeriodInDays(Duration.ofDays(model.retentionPeriodDays));
        }
        if (model.location != null) {
            builder.location(model.location);
        }
        if (model.prefixMismatchMode != null) {
            builder.prefixMismatchMode(model.prefixMismatchMode);
        }
        return builder.build();
    }

    private RewriteManifestsConfig convertRewriteManifests(RewriteManifestsModel model) {
        if (model == null) {
            return null;
        }
        RewriteManifestsConfig.Builder builder = RewriteManifestsConfig.builder();

        if (model.specId != null) {
            builder.specId(model.specId);
        }
        if (model.stagingLocation != null) {
            builder.stagingLocation(model.stagingLocation);
        }
        if (model.sortBy != null) {
            builder.sortBy(model.sortBy);
        }
        return builder.build();
    }

    private ScheduleConfig convertSchedule(ScheduleModel model) {
        if (model == null) {
            return null;
        }
        ScheduleConfig.Builder builder = ScheduleConfig.builder();
        if (model.intervalDays != null
                || model.intervalHours != null
                || model.intervalMinutes != null) {
            long days = model.intervalDays != null ? model.intervalDays : 0;
            long hours = model.intervalHours != null ? model.intervalHours : 0;
            long minutes = model.intervalMinutes != null ? model.intervalMinutes : 0;
            builder.interval(Duration.ofDays(days).plusHours(hours).plusMinutes(minutes));
        }
        if (model.cronExpression != null) {
            builder.cronExpression(model.cronExpression);
        }
        if (model.windowStart != null) {
            builder.windowStart(java.time.LocalTime.parse(model.windowStart));
        }
        if (model.windowEnd != null) {
            builder.windowEnd(java.time.LocalTime.parse(model.windowEnd));
        }
        if (model.timeoutHours != null) {
            builder.timeout(Duration.ofHours(model.timeoutHours));
        }
        if (model.priority != null) {
            builder.priority(model.priority);
        }
        if (model.enabled != null) {
            builder.enabled(model.enabled);
        }
        return builder.build();
    }

    // === YAML Model Classes ===
    static class PolicyFileModel {

        public List<PolicyModel> policies;
    }

    static class PolicyModel {

        public String name;
        public String description;
        public String pattern;
        public Boolean enabled;
        public Integer priority;
        public Map<String, String> tags;
        public RewriteDataFilesModel rewriteDataFiles;
        public ScheduleModel rewriteDataFilesSchedule;
        public ExpireSnapshotsModel expireSnapshots;
        public ScheduleModel expireSnapshotsSchedule;
        public OrphanCleanupModel orphanCleanup;
        public ScheduleModel orphanCleanupSchedule;
        public RewriteManifestsModel rewriteManifests;
        public ScheduleModel rewriteManifestsSchedule;
        public Instant createdAt;
        public Instant updatedAt;
    }

    static class ScheduleModel {

        public Integer intervalHours;
        public Integer intervalMinutes;
        public Integer intervalDays;
        public String cronExpression;
        public String windowStart;
        public String windowEnd;
        public Integer timeoutHours;
        public Integer priority;
        public Boolean enabled;
    }

    static class RewriteDataFilesModel {

        public String strategy;
        public List<String> sortOrder;
        public List<String> zOrderColumns;
        public Long targetFileSizeBytes;
        public Long maxFileGroupSizeBytes;
        public Integer maxConcurrentFileGroupRewrites;
        public Boolean partialProgressEnabled;
        public Integer partialProgressMaxCommits;
        public Integer partialProgressMaxFailedCommits;
        public String filter;
        public String rewriteJobOrder;
        public Boolean useStartingSequenceNumber;
        public Boolean removeDanglingDeletes;
        public Integer outputSpecId;
    }

    static class ExpireSnapshotsModel {

        public Integer retainLast;
        public Integer maxSnapshotAgeDays;
        public Boolean cleanExpiredMetadata;
    }

    static class OrphanCleanupModel {

        public Integer retentionPeriodDays;
        public String location;
        public String prefixMismatchMode;
    }

    static class RewriteManifestsModel {

        public Integer specId;
        public String stagingLocation;
        public List<String> sortBy;
    }
}
