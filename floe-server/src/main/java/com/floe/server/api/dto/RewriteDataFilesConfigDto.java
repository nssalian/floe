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
