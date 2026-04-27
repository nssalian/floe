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

package com.floe.core.auth;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable audit log entry for compliance and security monitoring.
 *
 * @param id unique identifier
 * @param timestamp when event occurred
 * @param eventType event type (AUTH_SUCCESS, AUTH_FAILURE, etc.)
 * @param eventDescription description
 * @param severity log level (INFO, WARNING, ERROR)
 * @param userId user or API key ID
 * @param username user or key name
 * @param authMethod authentication method used
 * @param resource accessed resource path
 * @param httpMethod HTTP method
 * @param ipAddress client IP
 * @param userAgent client user agent
 * @param details additional structured data
 * @param createdAt when record was persisted
 */
public record AuditLog(
        Long id,
        Instant timestamp,
        String eventType,
        String eventDescription,
        String severity,
        String userId,
        String username,
        String authMethod,
        String resource,
        String httpMethod,
        String ipAddress,
        String userAgent,
        Map<String, Object> details,
        Instant createdAt) {
    /**
     * Builder for creating new audit log entries
     *
     * @return Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Long id;
        private Instant timestamp = Instant.now();
        private String eventType;
        private String eventDescription;
        private String severity = "INFO";
        private String userId;
        private String username;
        private String authMethod;
        private String resource;
        private String httpMethod;
        private String ipAddress;
        private String userAgent;
        private Map<String, Object> details;
        private Instant createdAt = Instant.now();

        public Builder id(Long id) {
            this.id = id;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder eventDescription(String eventDescription) {
            this.eventDescription = eventDescription;
            return this;
        }

        public Builder severity(String severity) {
            this.severity = severity;
            return this;
        }

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder authMethod(String authMethod) {
            this.authMethod = authMethod;
            return this;
        }

        public Builder resource(String resource) {
            this.resource = resource;
            return this;
        }

        public Builder httpMethod(String httpMethod) {
            this.httpMethod = httpMethod;
            return this;
        }

        public Builder ipAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public Builder details(Map<String, Object> details) {
            this.details = details;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public AuditLog build() {
            return new AuditLog(
                    id,
                    timestamp,
                    eventType,
                    eventDescription,
                    severity,
                    userId,
                    username,
                    authMethod,
                    resource,
                    httpMethod,
                    ipAddress,
                    userAgent,
                    details,
                    createdAt);
        }
    }
}
