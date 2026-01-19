package com.floe.core.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default EventEmitter that logs events.
 *
 * <p>This is the default implementation when no external event system is configured. It logs events
 * at INFO level for completed operations and WARN level for failures.
 *
 * <p>To add webhook, Kafka, or SNS support, implement {@link EventEmitter} and register via CDI.
 */
public class LoggingEventEmitter implements EventEmitter {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingEventEmitter.class);

    private final boolean enabled;

    public LoggingEventEmitter() {
        this(true);
    }

    public LoggingEventEmitter(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public void emit(MaintenanceEvent event) {
        if (!enabled) {
            return;
        }

        switch (event.type()) {
            case OPERATION_STARTED ->
                    LOG.info(
                            "Event: {} - operation={} table={}.{} operationId={}",
                            event.type(),
                            event.operation(),
                            event.catalog(),
                            event.table(),
                            event.operationId());

            case OPERATION_SUCCEEDED ->
                    LOG.info(
                            "Event: {} - operation={} table={}.{} operationId={} metadata={}",
                            event.type(),
                            event.operation(),
                            event.catalog(),
                            event.table(),
                            event.operationId(),
                            event.metadata());

            case OPERATION_FAILED ->
                    LOG.warn(
                            "Event: {} - operation={} table={}.{} operationId={} error={}",
                            event.type(),
                            event.operation(),
                            event.catalog(),
                            event.table(),
                            event.operationId(),
                            event.errorMessage().orElse("unknown"));

            case POLICY_CREATED, POLICY_UPDATED ->
                    LOG.info(
                            "Event: {} - policyId={} metadata={}",
                            event.type(),
                            event.operationId(),
                            event.metadata());

            case POLICY_DELETED ->
                    LOG.info("Event: {} - policyId={}", event.type(), event.operationId());
        }
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
}
