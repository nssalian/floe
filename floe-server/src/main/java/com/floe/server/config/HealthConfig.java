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
