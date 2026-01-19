package com.floe.engine.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.floe.core.maintenance.ExpireSnapshotsOperation;
import com.floe.core.maintenance.OrphanCleanupOperation;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DurationSerializationTest {

    @Test
    void testExpireSnapshotsOperationSerialization() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());

        ExpireSnapshotsOperation op =
                ExpireSnapshotsOperation.builder()
                        .retainLast(5)
                        .maxSnapshotAge(Duration.ofDays(7))
                        .cleanExpiredMetadata(true)
                        .build();

        String json = mapper.writeValueAsString(op);

        JsonNode node = mapper.readTree(json);

        // MaintenanceJob reads 'maxSnapshotAge' - Duration serialized as seconds
        assertThat(node.has("maxSnapshotAge")).as("Should have 'maxSnapshotAge' field").isTrue();
        // 7 days = 604800 seconds
        assertThat(node.get("maxSnapshotAge").asDouble())
                .as("Duration should be serialized as seconds")
                .isEqualTo(604800.0);
    }

    @Test
    void testOrphanCleanupOperationSerialization() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());

        OrphanCleanupOperation op =
                OrphanCleanupOperation.builder()
                        .olderThan(Duration.ofDays(3))
                        .prefixMismatchMode(OrphanCleanupOperation.PrefixMismatchMode.ERROR)
                        .equalSchemes(Map.of("s3a", "s3"))
                        .equalAuthorities(Map.of())
                        .build();

        String json = mapper.writeValueAsString(op);

        JsonNode node = mapper.readTree(json);

        // MaintenanceJob reads 'olderThan' - Duration serialized as seconds
        assertThat(node.has("olderThan")).as("Should have 'olderThan' field").isTrue();
        // 3 days = 259200 seconds
        assertThat(node.get("olderThan").asDouble())
                .as("Duration should be serialized as seconds")
                .isEqualTo(259200.0);
    }
}
