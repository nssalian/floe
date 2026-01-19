package com.floe.core.auth;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents an API key for authentication.
 *
 * <p>SHA-256 hash of the key is persisted.
 *
 * @param id Unique identifier for the key
 * @param keyHash SHA-256 hash of the actual key
 * @param name Human-readable name for identification
 * @param role Role determining permissions
 * @param enabled Whether the key is active
 * @param createdAt When the key was created
 * @param expiresAt Optional expiration time (null = never expires)
 * @param lastUsedAt Last time the key was used for authentication
 * @param createdBy ID of the key that created this key (null for bootstrap)
 */
public record ApiKey(
        String id,
        String keyHash,
        String name,
        Role role,
        boolean enabled,
        Instant createdAt,
        Instant expiresAt,
        Instant lastUsedAt,
        String createdBy) {
    /** Key prefix for easy identification. */
    public static final String KEY_PREFIX = "floe_";

    /** Length of the random portion of the key. */
    public static final int KEY_RANDOM_LENGTH = 32;

    public boolean isExpired() {
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }

    public boolean isValid() {
        return enabled && !isExpired();
    }

    public boolean hasPermission(Permission permission) {
        return role.hasPermission(permission);
    }

    public ApiKey withLastUsedAt(Instant lastUsedAt) {
        return new ApiKey(
                id, keyHash, name, role, enabled, createdAt, expiresAt, lastUsedAt, createdBy);
    }

    public ApiKey withEnabled(boolean enabled) {
        return new ApiKey(
                id, keyHash, name, role, enabled, createdAt, expiresAt, lastUsedAt, createdBy);
    }

    public ApiKey withRole(Role role) {
        return new ApiKey(
                id, keyHash, name, role, enabled, createdAt, expiresAt, lastUsedAt, createdBy);
    }

    public ApiKey withName(String name) {
        return new ApiKey(
                id, keyHash, name, role, enabled, createdAt, expiresAt, lastUsedAt, createdBy);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id = UUID.randomUUID().toString();
        private String keyHash;
        private String name;
        private Role role = Role.VIEWER;
        private boolean enabled = true;
        private Instant createdAt = Instant.now();
        private Instant expiresAt;
        private Instant lastUsedAt;
        private String createdBy;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder keyHash(String keyHash) {
            this.keyHash = keyHash;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder role(Role role) {
            this.role = role;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder expiresAt(Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public Builder lastUsedAt(Instant lastUsedAt) {
            this.lastUsedAt = lastUsedAt;
            return this;
        }

        public Builder createdBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public ApiKey build() {
            if (keyHash == null || keyHash.isBlank()) {
                throw new IllegalArgumentException("keyHash is required");
            }
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("name is required");
            }
            if (role == null) {
                throw new IllegalArgumentException("role is required");
            }
            return new ApiKey(
                    id, keyHash, name, role, enabled, createdAt, expiresAt, lastUsedAt, createdBy);
        }
    }
}
