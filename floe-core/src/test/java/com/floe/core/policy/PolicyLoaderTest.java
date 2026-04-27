/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PolicyLoaderTest {

    private PolicyLoader loader;

    @BeforeEach
    void setUp() {
        loader = new PolicyLoader();
    }

    @Test
    void loadBasicPolicy() throws IOException {
        String yaml =
                """
            policies:
              - name: basic-policy
                description: A simple test policy
                pattern: "prod.analytics.*"
                priority: 10
                enabled: true
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);
        assertEquals("basic-policy", policy.name());
        assertEquals("A simple test policy", policy.description());
        assertEquals("prod", policy.tablePattern().catalogPattern());
        assertEquals("analytics", policy.tablePattern().namespacePattern());
        assertNull(policy.tablePattern().tablePattern());
        assertEquals(10, policy.priority());
        assertTrue(policy.enabled());
    }

    @Test
    void loadPolicyWithRewriteDataFiles() throws IOException {
        String yaml =
                """
            policies:
              - name: rewrite-data-files-policy
                pattern: "prod.*.*"
                rewrite-data-files:
                  strategy: binpack
                  target-file-size-bytes: 536870912
                  max-concurrent-file-group-rewrites: 3
                  partial-progress-enabled: true
                  partial-progress-max-commits: 5
                  rewrite-job-order: bytes-asc
                rewrite-data-files-schedule:
                  interval-hours: 4
                  timeout-hours: 2
                  enabled: true
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);

        assertNotNull(policy.rewriteDataFiles());
        assertEquals("BINPACK", policy.rewriteDataFiles().strategy());
        assertEquals(536870912L, policy.rewriteDataFiles().targetFileSizeBytes());
        assertEquals(3, policy.rewriteDataFiles().maxConcurrentFileGroupRewrites());
        assertTrue(policy.rewriteDataFiles().partialProgressEnabled());
        assertEquals(5, policy.rewriteDataFiles().partialProgressMaxCommits());
        assertEquals("bytes-asc", policy.rewriteDataFiles().rewriteJobOrder());

        assertNotNull(policy.rewriteDataFilesSchedule());
        assertTrue(policy.rewriteDataFilesSchedule().enabled());
    }

    @Test
    void loadPolicyWithAllRewriteDataFilesOptions() throws IOException {
        String yaml =
                """
            policies:
              - name: full-rewrite-policy
                pattern: "prod.*.*"
                rewrite-data-files:
                  strategy: sort
                  sort-order:
                    - col1
                    - col2
                  z-order-columns:
                    - col3
                  target-file-size-bytes: 536870912
                  max-file-group-size-bytes: 107374182400
                  max-concurrent-file-group-rewrites: 10
                  partial-progress-enabled: true
                  partial-progress-max-commits: 50
                  partial-progress-max-failed-commits: 3
                  filter: "date > '2025-01-01'"
                  rewrite-job-order: files-desc
                  use-starting-sequence-number: true
                  remove-dangling-deletes: true
                  output-spec-id: 1
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);
        RewriteDataFilesConfig config = policy.rewriteDataFiles();

        assertNotNull(config);
        assertEquals("SORT", config.strategy());
        assertEquals(List.of("col1", "col2"), config.sortOrder());
        assertEquals(List.of("col3"), config.zOrderColumns());
        assertEquals(536870912L, config.targetFileSizeBytes());
        assertEquals(107374182400L, config.maxFileGroupSizeBytes());
        assertEquals(10, config.maxConcurrentFileGroupRewrites());
        assertTrue(config.partialProgressEnabled());
        assertEquals(50, config.partialProgressMaxCommits());
        assertEquals(3, config.partialProgressMaxFailedCommits());
        assertEquals("date > '2025-01-01'", config.filter());
        assertEquals("files-desc", config.rewriteJobOrder());
        assertTrue(config.useStartingSequenceNumber());
        assertTrue(config.removeDanglingDeletes());
        assertEquals(1, config.outputSpecId());
    }

    @Test
    void loadPolicyWithExpireSnapshots() throws IOException {
        String yaml =
                """
            policies:
              - name: expire-snapshots-policy
                pattern: "*.*.*"
                expire-snapshots:
                  max-snapshot-age-days: 7
                  retain-last: 5
                expire-snapshots-schedule:
                  interval-hours: 24
                  window-start: "02:00"
                  window-end: "06:00"
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);

        assertNotNull(policy.expireSnapshots());
        assertEquals(7, policy.expireSnapshots().maxSnapshotAge().toDays());
        assertEquals(5, policy.expireSnapshots().retainLast());

        assertNotNull(policy.expireSnapshotsSchedule());
    }

    @Test
    void loadPolicyWithOrphanCleanup() throws IOException {
        String yaml =
                """
            policies:
              - name: orphan-cleanup-policy
                pattern: "prod.*.*"
                orphan-cleanup:
                  retention-period-days: 5
                  location: "s3://bucket/data"
                  prefix-mismatch-mode: ERROR
                orphan-cleanup-schedule:
                  interval-days: 7
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);

        assertNotNull(policy.orphanCleanup());
        assertEquals(5, policy.orphanCleanup().retentionPeriodInDays().toDays());
        assertEquals("s3://bucket/data", policy.orphanCleanup().location());
        assertEquals("ERROR", policy.orphanCleanup().prefixMismatchMode());
    }

    @Test
    void loadPolicyWithRewriteManifests() throws IOException {
        String yaml =
                """
            policies:
              - name: manifest-rewrite-policy
                pattern: "prod.*.*"
                rewrite-manifests:
                  spec-id: 1
                  staging-location: /data/staging
                  sort-by:
                    - date
                    - region
                rewrite-manifests-schedule:
                  interval-hours: 12
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);

        assertNotNull(policy.rewriteManifests());
        assertEquals(1, policy.rewriteManifests().specId());
        assertEquals("/data/staging", policy.rewriteManifests().stagingLocation());
        assertEquals(List.of("date", "region"), policy.rewriteManifests().sortBy());
    }

    @Test
    void loadMultiplePolicies() throws IOException {
        String yaml =
                """
            policies:
              - name: global-default
                pattern: "*.*.*"
                priority: 0

              - name: prod-default
                pattern: "prod.*.*"
                priority: 10

              - name: prod-analytics
                pattern: "prod.analytics.*"
                priority: 20
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(3, policies.size());
    }

    @Test
    void loadFromFile(@TempDir Path tempDir) throws IOException {
        String yaml =
                """
            policies:
              - name: file-policy
                pattern: "prod.*.*"
            """;

        Path policyFile = tempDir.resolve("policies.yaml");
        Files.writeString(policyFile, yaml);

        List<MaintenancePolicy> policies = loader.loadFromFile(policyFile);

        assertEquals(1, policies.size());
        assertEquals("file-policy", policies.get(0).name());
    }

    @Test
    void loadFromDirectory(@TempDir Path tempDir) throws IOException {
        String yaml1 =
                """
            policies:
              - name: policy-from-file1
                pattern: "prod.*.*"
            """;
        String yaml2 =
                """
            policies:
              - name: policy-from-file2
                pattern: "dev.*.*"
            """;

        Files.writeString(tempDir.resolve("01-prod.yaml"), yaml1);
        Files.writeString(tempDir.resolve("02-dev.yml"), yaml2);

        List<MaintenancePolicy> policies = loader.loadFromDirectory(tempDir);

        assertEquals(2, policies.size());
    }

    @Test
    void loadIntoStore(@TempDir Path tempDir) throws IOException {
        String yaml =
                """
            policies:
              - name: policy1
                pattern: "prod.*.*"
              - name: policy2
                pattern: "dev.*.*"
            """;

        Path policyFile = tempDir.resolve("policies.yaml");
        Files.writeString(policyFile, yaml);

        PolicyStore store = new InMemoryPolicyStore();
        loader.loadIntoStore(policyFile, store);

        assertEquals(2, store.count());
        assertTrue(store.existsByName("policy1"));
        assertTrue(store.existsByName("policy2"));
    }

    @Test
    void loadFullPolicy() throws IOException {
        String yaml =
                """
            policies:
              - name: comprehensive-policy
                description: A fully configured policy for high-traffic tables
                pattern: "prod.streaming.*"
                priority: 100
                enabled: true
                tags:
                  team: data-platform
                  tier: critical

                rewrite-data-files:
                  strategy: binpack
                  target-file-size-bytes: 536870912
                  max-file-group-size-bytes: 107374182400
                  max-concurrent-file-group-rewrites: 10
                  partial-progress-enabled: true
                  partial-progress-max-commits: 20
                  partial-progress-max-failed-commits: 5
                  rewrite-job-order: bytes-asc
                  remove-dangling-deletes: true

                rewrite-data-files-schedule:
                  interval-hours: 2
                  window-start: "00:00"
                  window-end: "06:00"
                  timeout-hours: 1
                  priority: 10
                  enabled: true

                expire-snapshots:
                  max-snapshot-age-days: 3
                  retain-last: 2
                  clean-expired-metadata: true

                expire-snapshots-schedule:
                  interval-hours: 6
                  enabled: true

                orphan-cleanup:
                  retention-period-days: 5

                orphan-cleanup-schedule:
                  interval-days: 7
                  enabled: true

                rewrite-manifests:
                  spec-id: 0
                  staging-location: /staging

                rewrite-manifests-schedule:
                  interval-hours: 12
                  enabled: true
            """;

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, policies.size());
        MaintenancePolicy policy = policies.get(0);

        // Verify all sections are loaded
        assertEquals("comprehensive-policy", policy.name());
        assertEquals("A fully configured policy for high-traffic tables", policy.description());
        assertEquals(100, policy.priority());
        assertTrue(policy.enabled());
        assertEquals("data-platform", policy.tags().get("team"));
        assertEquals("critical", policy.tags().get("tier"));

        assertNotNull(policy.rewriteDataFiles());
        assertNotNull(policy.rewriteDataFilesSchedule());
        assertNotNull(policy.expireSnapshots());
        assertNotNull(policy.expireSnapshotsSchedule());
        assertNotNull(policy.orphanCleanup());
        assertNotNull(policy.orphanCleanupSchedule());
        assertNotNull(policy.rewriteManifests());
        assertNotNull(policy.rewriteManifestsSchedule());

        // Verify specific values
        assertEquals("BINPACK", policy.rewriteDataFiles().strategy());
        assertEquals(536870912L, policy.rewriteDataFiles().targetFileSizeBytes());
        assertTrue(policy.rewriteDataFiles().removeDanglingDeletes());

        assertEquals(3, policy.expireSnapshots().maxSnapshotAge().toDays());
        assertEquals(2, policy.expireSnapshots().retainLast());
    }

    @Test
    void emptyPoliciesFile() throws IOException {
        String yaml = "policies: []";

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertTrue(policies.isEmpty());
    }

    @Test
    void nullPoliciesFile() throws IOException {
        String yaml = "policies:";

        List<MaintenancePolicy> policies =
                loader.loadFromStream(
                        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertTrue(policies.isEmpty());
    }
}
