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

import com.floe.core.policy.OrphanCleanupConfig;
import java.time.Duration;
import java.util.Map;

/** DTO for OrphanCleanup configuration. */
public record OrphanCleanupConfigDto(
        Integer retentionPeriodInDays,
        String location,
        String prefixMismatchMode,
        Map<String, String> equalSchemes,
        Map<String, String> equalAuthorities) {
    public OrphanCleanupConfig toConfig() {
        return new OrphanCleanupConfig(
                retentionPeriodInDays != null
                        ? Duration.ofDays(retentionPeriodInDays)
                        : Duration.ofDays(3),
                location,
                prefixMismatchMode != null ? prefixMismatchMode : "ERROR",
                equalSchemes != null ? equalSchemes : Map.of(),
                equalAuthorities != null ? equalAuthorities : Map.of());
    }
}
