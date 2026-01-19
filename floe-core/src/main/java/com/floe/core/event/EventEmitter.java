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
