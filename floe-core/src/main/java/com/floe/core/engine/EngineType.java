package com.floe.core.engine;

/** Supported execution engine types. */
public enum EngineType {
    SPARK("Apache Spark", "Full Iceberg maintenance support"),
    TRINO("Trino", "OPTIMIZE and expire_snapshots via SQL");

    private final String displayName;
    private final String description;

    EngineType(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }
}
