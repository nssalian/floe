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

package com.floe.core.event;

/**
 * Interface for emitting maintenance events.
 *
 * <p>Configure via:
 *
 * <pre>
 * floe:
 *   events:
 *     enabled: true
 *     type: logging
 * </pre>
 */
public interface EventEmitter {

    /**
     * Emit a maintenance event.
     *
     * <p>Implementations should be non-blocking and handle failures gracefully (log and continue).
     *
     * @param event the event to emit
     */
    void emit(MaintenanceEvent event);

    /**
     * Check if the emitter is enabled.
     *
     * @return true if events will be emitted, false if no-op
     */
    default boolean isEnabled() {
        return true;
    }
}
