package com.floe.server.api.dto;

import com.floe.core.policy.RewriteDataFilesConfig;
import java.util.List;

/** DTO for RewriteDataFiles configuration. */
public record RewriteDataFilesConfigDto(
        String strategy,
        List<String> sortOrder,
        List<String> zOrderColumns,
        Long targetFileSizeBytes,
        Long maxFileGroupSizeBytes,
        Integer maxConcurrentFileGroupRewrites,
        Boolean partialProgressEnabled,
        Integer partialProgressMaxCommits,
        Integer partialProgressMaxFailedCommits,
        String filter,
        String rewriteJobOrder,
        Boolean useStartingSequenceNumber,
        Boolean removeDanglingDeletes,
        Integer outputSpecId) {
    public RewriteDataFilesConfig toConfig() {
        return RewriteDataFilesConfig.builder()
                .strategy(strategy != null ? strategy : "BINPACK")
                .sortOrder(sortOrder)
                .zOrderColumns(zOrderColumns)
                .targetFileSizeBytes(targetFileSizeBytes)
                .maxFileGroupSizeBytes(maxFileGroupSizeBytes)
                .maxConcurrentFileGroupRewrites(maxConcurrentFileGroupRewrites)
                .partialProgressEnabled(partialProgressEnabled)
                .partialProgressMaxCommits(partialProgressMaxCommits)
                .partialProgressMaxFailedCommits(partialProgressMaxFailedCommits)
                .filter(filter)
                .rewriteJobOrder(rewriteJobOrder)
                .useStartingSequenceNumber(useStartingSequenceNumber)
                .removeDanglingDeletes(removeDanglingDeletes)
                .outputSpecId(outputSpecId)
                .build();
    }
}
