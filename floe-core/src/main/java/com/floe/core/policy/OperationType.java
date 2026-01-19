package com.floe.core.policy;

/** Types of Iceberg table maintenance operations. */
public enum OperationType {
    /** Compact small data files into larger files using rewriteDataFiles. */
    REWRITE_DATA_FILES,

    /** Remove old snapshots and their unreferenced data files. */
    EXPIRE_SNAPSHOTS,

    /** Delete files not referenced by any table metadata. */
    ORPHAN_CLEANUP,

    /** Rewrite manifest files to optimize metadata structure. */
    REWRITE_MANIFESTS,
}
