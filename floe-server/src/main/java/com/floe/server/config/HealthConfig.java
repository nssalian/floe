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

package com.floe.server.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Type-safe configuration for health assessment. */
@ConfigMapping(prefix = "floe.health")
@StaticInitSafe
public interface HealthConfig {
    /**
     * Scan mode for health assessment: metadata, scan, or sample.
     *
     * <p>Default: metadata
     */
    @WithDefault("metadata")
    String scanMode();

    /** Maximum number of files to sample when scanMode is "sample". Default: 10000 */
    @WithDefault("10000")
    int sampleLimit();

    /** Enable health report persistence. Default: true */
    @WithDefault("true")
    boolean persistenceEnabled();

    /** Maximum number of health reports to retain per table. Default: 100 */
    @WithDefault("100")
    int maxReportsPerTable();

    /** Maximum age of health reports to retain, in days. Default: 30 (0 disables age pruning). */
    @WithDefault("30")
    int maxReportAgeDays();
}
