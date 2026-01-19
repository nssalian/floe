package com.floe.core.scheduler;

import java.time.Duration;
import java.util.Optional;

/** Interface for distributed locking to ensure at-most-once execution. */
public interface DistributedLock {

    /**
     * Try to acquire a named lock.
     *
     * @param lockName unique name for the lock (e.g., "floe-scheduler")
     * @param ttl maximum time to hold the lock before automatic release
     * @return handle to release the lock, or empty if lock is held by another process
     */
    Optional<LockHandle> tryAcquire(String lockName, Duration ttl);

    /**
     * Check if a lock is currently held (by any process).
     *
     * @param lockName the lock name
     * @return true if the lock is held
     */
    boolean isLocked(String lockName);

    /** Handle for an acquired lock. Must be closed to release the lock. */
    interface LockHandle extends AutoCloseable {
        /** Get the lock name. */
        String lockName();

        /** Check if this handle still holds the lock. */
        boolean isValid();

        /**
         * Extend the lock TTL.
         *
         * @param extension additional time to hold the lock
         * @return true if extension was successful
         */
        boolean extend(Duration extension);

        /** Release the lock. */
        @Override
        void close();
    }

    /**
     * No-op implementation for single-replica deployments.
     *
     * <p>This is the default implementation used when no distributed lock is configured. Relying on
     * the JVM-local AtomicBoolean.
     */
    class NoOpDistributedLock implements DistributedLock {
        @Override
        public Optional<LockHandle> tryAcquire(String lockName, Duration ttl) {
            return Optional.of(new NoOpLockHandle(lockName));
        }

        @Override
        public boolean isLocked(String lockName) {
            return false;
        }

        private record NoOpLockHandle(String lockName) implements LockHandle {
            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public boolean extend(Duration extension) {
                return true;
            }

            @Override
            public void close() {
                // No-op
            }
        }
    }
}
