package com.floe.core.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DistributedLockTest {

    @Nested
    @DisplayName("NoOpDistributedLock")
    class NoOpDistributedLockTests {

        private DistributedLock.NoOpDistributedLock lock;

        @BeforeEach
        void setUp() {
            lock = new DistributedLock.NoOpDistributedLock();
        }

        @Test
        @DisplayName("tryAcquire always succeeds")
        void tryAcquire_alwaysSucceeds() {
            Optional<DistributedLock.LockHandle> handle =
                    lock.tryAcquire("test-lock", Duration.ofMinutes(5));

            assertThat(handle).isPresent();
        }

        @Test
        @DisplayName("tryAcquire returns handle with correct lock name")
        void tryAcquire_returnsHandleWithCorrectName() {
            String lockName = "my-scheduler-lock";
            Optional<DistributedLock.LockHandle> handle =
                    lock.tryAcquire(lockName, Duration.ofMinutes(5));

            assertThat(handle).isPresent();
            assertThat(handle.get().lockName()).isEqualTo(lockName);
        }

        @Test
        @DisplayName("tryAcquire works with any TTL duration")
        void tryAcquire_worksWithAnyTtl() {
            assertThat(lock.tryAcquire("lock1", Duration.ofSeconds(1))).isPresent();
            assertThat(lock.tryAcquire("lock2", Duration.ofHours(24))).isPresent();
            assertThat(lock.tryAcquire("lock3", Duration.ZERO)).isPresent();
        }

        @Test
        @DisplayName("multiple acquires of same lock all succeed")
        void tryAcquire_multipleAcquiresSameLockSucceed() {
            Optional<DistributedLock.LockHandle> handle1 =
                    lock.tryAcquire("same-lock", Duration.ofMinutes(5));
            Optional<DistributedLock.LockHandle> handle2 =
                    lock.tryAcquire("same-lock", Duration.ofMinutes(5));
            Optional<DistributedLock.LockHandle> handle3 =
                    lock.tryAcquire("same-lock", Duration.ofMinutes(5));

            assertThat(handle1).isPresent();
            assertThat(handle2).isPresent();
            assertThat(handle3).isPresent();
        }

        @Test
        @DisplayName("isLocked always returns false")
        void isLocked_alwaysReturnsFalse() {
            assertThat(lock.isLocked("any-lock")).isFalse();
            assertThat(lock.isLocked("floe-scheduler")).isFalse();
            assertThat(lock.isLocked("")).isFalse();
        }

        @Test
        @DisplayName("isLocked returns false even after acquiring lock")
        void isLocked_returnsFalseEvenAfterAcquiring() {
            lock.tryAcquire("test-lock", Duration.ofMinutes(5));

            // NoOp doesn't track locks, so still returns false
            assertThat(lock.isLocked("test-lock")).isFalse();
        }
    }

    @Nested
    @DisplayName("NoOpLockHandle")
    class NoOpLockHandleTests {

        private DistributedLock.LockHandle handle;

        @BeforeEach
        void setUp() {
            DistributedLock.NoOpDistributedLock lock = new DistributedLock.NoOpDistributedLock();
            handle = lock.tryAcquire("test-lock", Duration.ofMinutes(5)).orElseThrow();
        }

        @Test
        @DisplayName("lockName returns the lock name")
        void lockName_returnsLockName() {
            assertThat(handle.lockName()).isEqualTo("test-lock");
        }

        @Test
        @DisplayName("isValid always returns true")
        void isValid_alwaysReturnsTrue() {
            assertThat(handle.isValid()).isTrue();
        }

        @Test
        @DisplayName("isValid returns true even after close")
        void isValid_returnsTrueEvenAfterClose() {
            handle.close();

            // NoOp handle doesn't track state
            assertThat(handle.isValid()).isTrue();
        }

        @Test
        @DisplayName("extend always returns true")
        void extend_alwaysReturnsTrue() {
            assertThat(handle.extend(Duration.ofMinutes(10))).isTrue();
            assertThat(handle.extend(Duration.ofHours(1))).isTrue();
            assertThat(handle.extend(Duration.ZERO)).isTrue();
        }

        @Test
        @DisplayName("close is idempotent")
        void close_isIdempotent() {
            // Should not throw
            handle.close();
            handle.close();
            handle.close();
        }

        @Test
        @DisplayName("can be used in try-with-resources")
        void canBeUsedInTryWithResources() {
            DistributedLock.NoOpDistributedLock lock = new DistributedLock.NoOpDistributedLock();

            try (DistributedLock.LockHandle h =
                    lock.tryAcquire("auto-close-lock", Duration.ofMinutes(5)).orElseThrow()) {
                assertThat(h.isValid()).isTrue();
                assertThat(h.lockName()).isEqualTo("auto-close-lock");
            }
            // No exception means success
        }
    }

    @Nested
    @DisplayName("Interface contract")
    class InterfaceContractTests {

        @Test
        @DisplayName("NoOpDistributedLock implements DistributedLock")
        void noOpImplementsInterface() {
            DistributedLock lock = new DistributedLock.NoOpDistributedLock();

            assertThat(lock).isInstanceOf(DistributedLock.class);
        }

        @Test
        @DisplayName("LockHandle is AutoCloseable")
        void lockHandleIsAutoCloseable() {
            DistributedLock lock = new DistributedLock.NoOpDistributedLock();
            DistributedLock.LockHandle handle =
                    lock.tryAcquire("test", Duration.ofMinutes(1)).orElseThrow();

            assertThat(handle).isInstanceOf(AutoCloseable.class);
        }

        @Test
        @DisplayName("tryAcquire with different lock names returns independent handles")
        void tryAcquire_differentNamesReturnIndependentHandles() {
            DistributedLock lock = new DistributedLock.NoOpDistributedLock();

            DistributedLock.LockHandle handle1 =
                    lock.tryAcquire("lock-1", Duration.ofMinutes(5)).orElseThrow();
            DistributedLock.LockHandle handle2 =
                    lock.tryAcquire("lock-2", Duration.ofMinutes(5)).orElseThrow();

            assertThat(handle1.lockName()).isEqualTo("lock-1");
            assertThat(handle2.lockName()).isEqualTo("lock-2");
            assertThat(handle1).isNotSameAs(handle2);
        }
    }
}
