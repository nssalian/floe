-- Floe Database Initialization Script
--
-- Tables:
--   - api_keys: API key authentication and authorization
--   - maintenance_policies: Maintenance policy definitions
--   - maintenance_operations: Operation execution history
--   - schedule_executions: Schedule tracking for policies
--   - audit_logs: Immutable audit trail for compliance

-- Create the floe user and database
CREATE USER floe WITH PASSWORD 'floe';
CREATE DATABASE floe OWNER floe;
GRANT ALL PRIVILEGES ON DATABASE floe TO floe;

-- Connect to the floe database as the floe user for table creation
\connect floe floe

-- Grant schema privileges (in case needed)
GRANT ALL ON SCHEMA public TO floe;

-- API Keys Table
-- Stores API keys for authentication. Keys are stored as SHA-256 hashes.
-- Supports multi-tenancy via tenant_id column.

CREATE TABLE IF NOT EXISTS api_keys (
    id VARCHAR(36) PRIMARY KEY,
    key_hash VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL UNIQUE,
    role VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    created_by VARCHAR(36),
    tenant_id VARCHAR(255),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_name ON api_keys(name);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);

COMMENT ON TABLE api_keys IS 'API keys for authentication and authorization';
COMMENT ON COLUMN api_keys.key_hash IS 'SHA-256 hash of the API key';
COMMENT ON COLUMN api_keys.tenant_id IS 'Tenant identifier for multi-tenancy (null = global)';

-- Maintenance Policies Table
-- Stores maintenance policy definitions with their configurations and schedules.
-- Policies define what maintenance operations run on which tables and when.

CREATE TABLE IF NOT EXISTS maintenance_policies (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    table_pattern VARCHAR(512) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,

    -- Operation configurations (JSONB for flexibility)
    rewrite_data_files_config JSONB,
    rewrite_data_files_schedule JSONB,
    expire_snapshots_config JSONB,
    expire_snapshots_schedule JSONB,
    orphan_cleanup_config JSONB,
    orphan_cleanup_schedule JSONB,
    rewrite_manifests_config JSONB,
    rewrite_manifests_schedule JSONB,

    -- Metadata
    tags JSONB,
    tenant_id VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_policies_enabled ON maintenance_policies(enabled);
CREATE INDEX IF NOT EXISTS idx_policies_name ON maintenance_policies(name);
CREATE INDEX IF NOT EXISTS idx_policies_pattern ON maintenance_policies(table_pattern);
CREATE INDEX IF NOT EXISTS idx_policies_tenant ON maintenance_policies(tenant_id);

COMMENT ON TABLE maintenance_policies IS 'Maintenance policy definitions for Iceberg tables';
COMMENT ON COLUMN maintenance_policies.table_pattern IS 'Pattern matching tables (e.g., catalog.db.* or *.*.orders)';
COMMENT ON COLUMN maintenance_policies.priority IS 'Higher priority policies take precedence in conflicts';

-- Maintenance Operations Table
-- Stores execution history for maintenance operations.
-- Each record represents a single maintenance run on a table.

CREATE TABLE IF NOT EXISTS maintenance_operations (
    id UUID PRIMARY KEY,
    catalog VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    policy_name VARCHAR(255),
    policy_id UUID,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    results JSONB,
    error_message TEXT,
    tenant_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_operations_table ON maintenance_operations(catalog, namespace, table_name);
CREATE INDEX IF NOT EXISTS idx_operations_status ON maintenance_operations(status);
CREATE INDEX IF NOT EXISTS idx_operations_started_at ON maintenance_operations(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_operations_tenant ON maintenance_operations(tenant_id);

COMMENT ON TABLE maintenance_operations IS 'Execution history for maintenance operations';
COMMENT ON COLUMN maintenance_operations.status IS 'PENDING, RUNNING, SUCCESS, FAILED, PARTIAL_FAILURE, NO_POLICY, NO_OPERATIONS';

-- Schedule Executions Table
-- Tracks when policies were last executed and when they should run next.
-- Used by the scheduler to determine due operations.

CREATE TABLE IF NOT EXISTS schedule_executions (
    policy_id VARCHAR(255) NOT NULL,
    operation_type VARCHAR(50) NOT NULL,
    table_key VARCHAR(767) NOT NULL,
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (policy_id, operation_type, table_key)
);

CREATE INDEX IF NOT EXISTS idx_schedule_executions_policy ON schedule_executions(policy_id);
CREATE INDEX IF NOT EXISTS idx_schedule_executions_table_key ON schedule_executions(table_key);
CREATE INDEX IF NOT EXISTS idx_schedule_executions_next_run_at ON schedule_executions(next_run_at);

COMMENT ON TABLE schedule_executions IS 'Schedule tracking for policy executions';
COMMENT ON COLUMN schedule_executions.table_key IS 'Fully qualified table identifier (catalog.namespace.table)';

-- Audit Logs Table
-- Immutable audit trail for security events, authentication, and authorization.
-- Designed for SOC 2/GDPR compliance with append-only semantics.

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,

    -- Event metadata
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    event_type VARCHAR(100) NOT NULL,
    event_description TEXT NOT NULL,
    severity VARCHAR(10) NOT NULL CHECK (severity IN ('INFO', 'WARN', 'ERROR')),

    -- User/principal information
    user_id VARCHAR(255),
    username VARCHAR(255),
    auth_method VARCHAR(50),

    -- Multi-tenancy
    tenant_id VARCHAR(255),

    -- Request/resource context
    resource VARCHAR(500),
    http_method VARCHAR(10),
    ip_address VARCHAR(45),
    user_agent TEXT,

    -- Flexible details storage (JSON)
    details JSONB,

    -- Immutability guarantee
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Performance indexes for common query patterns
CREATE INDEX idx_audit_logs_timestamp ON audit_logs(timestamp DESC);
CREATE INDEX idx_audit_logs_user ON audit_logs(user_id, timestamp DESC) WHERE user_id IS NOT NULL;
CREATE INDEX idx_audit_logs_event ON audit_logs(event_type, timestamp DESC);
CREATE INDEX idx_audit_logs_tenant ON audit_logs(tenant_id, timestamp DESC) WHERE tenant_id IS NOT NULL;
CREATE INDEX idx_audit_logs_severity ON audit_logs(severity, timestamp DESC);
CREATE INDEX idx_audit_logs_ip ON audit_logs(ip_address, timestamp DESC) WHERE ip_address IS NOT NULL;

-- GIN index for JSONB queries
CREATE INDEX idx_audit_logs_details ON audit_logs USING GIN (details);

COMMENT ON TABLE audit_logs IS 'Immutable audit trail for security events. Retention: 7 years per GDPR/SOC 2.';
COMMENT ON COLUMN audit_logs.severity IS 'Log severity: INFO (success), WARN (violation), ERROR (system error)';

-- Create a view for recent audit events (last 30 days)
CREATE OR REPLACE VIEW recent_audit_logs AS
SELECT
    id,
    timestamp,
    event_type,
    event_description,
    severity,
    user_id,
    username,
    tenant_id,
    resource,
    ip_address,
    details
FROM audit_logs
WHERE timestamp >= NOW() - INTERVAL '30 days'
ORDER BY timestamp DESC;

COMMENT ON VIEW recent_audit_logs IS 'Audit logs from the last 30 days for quick access';

-- Trigger to prevent updates/deletes on audit logs (defense in depth)
CREATE OR REPLACE FUNCTION prevent_audit_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Audit logs are immutable and cannot be modified or deleted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_audit_update
    BEFORE UPDATE ON audit_logs
    FOR EACH ROW
    EXECUTE FUNCTION prevent_audit_log_modification();

CREATE TRIGGER trg_prevent_audit_delete
    BEFORE DELETE ON audit_logs
    FOR EACH ROW
    EXECUTE FUNCTION prevent_audit_log_modification();

-- Catalog Configs Table
-- Stores non-sensitive catalog configuration for display in the UI.
-- Credentials are NEVER stored here - they remain in environment variables.

CREATE TABLE IF NOT EXISTS catalog_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    uri VARCHAR(1024),
    warehouse VARCHAR(1024),
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    active BOOLEAN DEFAULT false
);

-- Only one active catalog at a time
CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_configs_active
    ON catalog_configs (active) WHERE active = true;

-- Unique constraint on name + type combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_configs_name_type
    ON catalog_configs (name, type);

COMMENT ON TABLE catalog_configs IS 'Non-sensitive catalog configuration for UI display';
COMMENT ON COLUMN catalog_configs.properties IS 'Non-sensitive properties (region, ref, path-style, etc.)';
COMMENT ON COLUMN catalog_configs.active IS 'Only one catalog can be active at a time';

-- Grant Permissions
-- Grant all permissions on tables to the floe user

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO floe;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO floe;

-- Schema Version Tracking (for future migrations if needed)

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO schema_version (version, description)
VALUES (1, 'Initial schema with all tables')
ON CONFLICT (version) DO NOTHING;

COMMENT ON TABLE schema_version IS 'Tracks schema version for manual migrations';
