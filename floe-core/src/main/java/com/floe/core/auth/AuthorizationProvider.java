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

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Interface for authorization decisions. */
public interface AuthorizationProvider {
    /**
     * Check if a subject is authorized to perform an action on a resource.
     *
     * @param request the authorization request
     * @return the authorization result
     */
    AuthorizationResult authorize(AuthorizationRequest request);

    /**
     * Filter a list of resources to only those the subject can access.
     *
     * <p>Default implementation calls {@link #authorize} for each resource.
     *
     * @param subject the subject (user, api key, etc.)
     * @param action the action to check
     * @param resources the resources to filter
     * @param resourceIdExtractor function to extract resource ID from each item
     * @param resourceType the type of resource
     * @param <T> the resource type
     * @return list of resources the subject can access
     */
    default <T> List<T> filterAuthorized(
            String subject,
            String action,
            List<T> resources,
            Function<T, String> resourceIdExtractor,
            String resourceType) {
        return resources.stream()
                .filter(
                        resource -> {
                            var request =
                                    new AuthorizationRequest(
                                            subject,
                                            action,
                                            resourceType,
                                            resourceIdExtractor.apply(resource),
                                            Map.of());
                            return authorize(request).allowed();
                        })
                .toList();
    }

    /**
     * Check if the provider is available and configured.
     *
     * @return true if authorization checks can be performed
     */
    default boolean isAvailable() {
        return true;
    }

    /** Authorization request containing subject, action, and resource details. */
    record AuthorizationRequest(
            String subject,
            String action,
            String resourceType,
            String resourceId,
            Map<String, Object> context) {
        /** Create a request with no context. */
        public static AuthorizationRequest of(
                String subject, String action, String resourceType, String resourceId) {
            return new AuthorizationRequest(subject, action, resourceType, resourceId, Map.of());
        }
    }

    /** Result of an authorization check. */
    record AuthorizationResult(boolean allowed, String reason) {
        public static final AuthorizationResult ALLOWED = new AuthorizationResult(true, "allowed");
        public static final AuthorizationResult DENIED =
                new AuthorizationResult(false, "access denied");

        public static AuthorizationResult allowed(String reason) {
            return new AuthorizationResult(true, reason);
        }

        public static AuthorizationResult denied(String reason) {
            return new AuthorizationResult(false, reason);
        }
    }
}
