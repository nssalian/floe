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

/** Interface for logging security audit events. Implementations should log to structured format */
public interface AuditLogger {

    /**
     * Log an audit event with context information
     *
     * @param event The type of audit event
     * @param context Additional context (userId, IP, resource, etc.)
     */
    void log(AuditEvent event, Object... context);

    /**
     * Log an audit event with principal and resource information
     *
     * @param event The type of audit event
     * @param principal The authenticated principal (user or service)
     * @param resource The resource being accessed (e.g., policy ID, table name)
     * @param result The result of the operation (success, failure, etc.)
     */
    void logAccess(AuditEvent event, FloePrincipal principal, String resource, String result);

    /**
     * Log a security event (failed auth, rate limit exceeded, etc.)
     *
     * @param event The type of security event
     * @param ipAddress Client IP address
     * @param details Additional details about the event
     */
    void logSecurityEvent(AuditEvent event, String ipAddress, String details);
}
