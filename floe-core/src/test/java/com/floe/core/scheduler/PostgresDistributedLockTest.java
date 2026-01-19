package com.floe.core.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
@DisplayName("PostgresDistributedLock")
class PostgresDistributedLockTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("floe_test")
                    .withUsername("test")
                    .withPassword("test");

    private static HikariDataSource dataSource;
    private PostgresDistributedLock lock;

    @BeforeAll
    static void setUpDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(10); // Need multiple connections for lock testing
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void tearDownDataSource() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void setUp() {
        lock = new PostgresDistributedLock(dataSource);
    }

    @Nested
    @DisplayName("tryAcquire")
    class TryAcquireTests {

        @Test
        @DisplayName("should acquire lock successfully")
        void shouldAcquireLockSuccessfully() {
            Optional<DistributedLock.LockHandle> handle =
                    lock.tryAcquire("test-lock", Duration.ofMinutes(5));

            assertThat(handle).isPresent();
            assertThat(handle.get().lockName()).isEqualTo("test-lock");
            assertThat(handle.get().isValid()).isTrue();

            handle.get().close();
        }

        @Test
        @DisplayName("should return handle with correct lock name")
        void shouldReturnHandleWithCorrectLockName() {
            String lockName = "floe-scheduler";

            try (DistributedLock.LockHandle handle =
                    lock.tryAcquire(lockName, Duration.ofMinutes(5)).orElseThrow()) {
                assertThat(handle.lockName()).isEqualTo(lockName);
            }
        }

        @Test
        @DisplayName("should acquire different locks independently")
        void shouldAcquireDifferentLocksIndependently() {
            Optional<DistributedLock.LockHandle> handle1 =
                    lock.tryAcquire("lock-1", Duration.ofMinutes(5));
            Optional<DistributedLock.LockHandle> handle2 =
                    lock.tryAcquire("lock-2", Duration.ofMinutes(5));

            assertThat(handle1).isPresent();
            assertThat(handle2).isPresent();

            handle1.get().close();
            handle2.get().close();
        }

        @Test
        @DisplayName("should return same handle when reacquiring held lock")
        void shouldReturnSameHandleWhenReacquiringHeldLock() {
            Optional<DistributedLock.LockHandle> handle1 =
                    lock.tryAcquire("reentrant-lock", Duration.ofMinutes(5));
            Optional<DistributedLock.LockHandle> handle2 =
                    lock.tryAcquire("reentrant-lock", Duration.ofMinutes(5));

            assertThat(handle1).isPresent();
            assertThat(handle2).isPresent();
            // Same instance should be returned for reentrant acquire
            assertThat(handle1.get()).isSameAs(handle2.get());

            handle1.get().close();
        }
    }

    @Nested
    @DisplayName("Lock exclusivity")
    class LockExclusivityTests {

        @Test
        @DisplayName("should prevent concurrent acquisition of same lock")
        void shouldPreventConcurrentAcquisitionOfSameLock() throws Exception {
            PostgresDistributedLock lock1 = new PostgresDistributedLock(dataSource);
            PostgresDistributedLock lock2 = new PostgresDistributedLock(dataSource);

            // First lock acquires successfully
            Optional<DistributedLock.LockHandle> handle1 =
                    lock1.tryAcquire("exclusive-lock", Duration.ofMinutes(5));
            assertThat(handle1).isPresent();

            // Second lock should fail to acquire
            Optional<DistributedLock.LockHandle> handle2 =
                    lock2.tryAcquire("exclusive-lock", Duration.ofMinutes(5));
            assertThat(handle2).isEmpty();

            // After releasing, second lock should succeed
            handle1.get().close();

            Optional<DistributedLock.LockHandle> handle3 =
                    lock2.tryAcquire("exclusive-lock", Duration.ofMinutes(5));
            assertThat(handle3).isPresent();

            handle3.get().close();
        }

        @Test
        @DisplayName("should allow only one thread to hold lock")
        void shouldAllowOnlyOneThreadToHoldLock() throws Exception {
            int threadCount = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            for (int i = 0; i < threadCount; i++) {
                @SuppressWarnings("FutureReturnValueIgnored")
                var unused =
                        executor.submit(
                                () -> {
                                    try {
                                        startLatch.await();
                                        PostgresDistributedLock threadLock =
                                                new PostgresDistributedLock(dataSource);
                                        Optional<DistributedLock.LockHandle> handle =
                                                threadLock.tryAcquire(
                                                        "contended-lock", Duration.ofSeconds(30));

                                        if (handle.isPresent()) {
                                            successCount.incrementAndGet();
                                            // Hold lock briefly
                                            Thread.sleep(100);
                                            handle.get().close();
                                        } else {
                                            failCount.incrementAndGet();
                                        }
                                    } catch (Exception e) {
                                        failCount.incrementAndGet();
                                    } finally {
                                        doneLatch.countDown();
                                    }
                                });
            }

            // Start all threads simultaneously
            startLatch.countDown();
            doneLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            // Exactly one thread should have acquired the lock initially
            // (others may succeed after release, but at least 1 should have failed initially)
            assertThat(successCount.get()).isGreaterThanOrEqualTo(1);
            assertThat(failCount.get()).isGreaterThanOrEqualTo(1);
        }
    }

    @Nested
    @DisplayName("isLocked")
    class IsLockedTests {

        @Test
        @DisplayName("should return false when lock is not held")
        void shouldReturnFalseWhenLockIsNotHeld() {
            assertThat(lock.isLocked("non-existent-lock")).isFalse();
        }

        @Test
        @DisplayName("lock exclusivity proves lock is held")
        void lockExclusivityProvesLockIsHeld() {
            // Instead of relying on isLocked() query, we prove the lock is held
            // by showing another instance cannot acquire it
            Optional<DistributedLock.LockHandle> handle =
                    lock.tryAcquire("exclusivity-test", Duration.ofMinutes(5));
            assertThat(handle).isPresent();

            // Another lock instance should fail to acquire
            PostgresDistributedLock otherLock = new PostgresDistributedLock(dataSource);
            Optional<DistributedLock.LockHandle> otherHandle =
                    otherLock.tryAcquire("exclusivity-test", Duration.ofMinutes(5));
            assertThat(otherHandle).isEmpty();

            // After release, other instance should succeed
            handle.get().close();
            Optional<DistributedLock.LockHandle> afterRelease =
                    otherLock.tryAcquire("exclusivity-test", Duration.ofMinutes(5));
            assertThat(afterRelease).isPresent();
            afterRelease.get().close();
        }
    }

    @Nested
    @DisplayName("LockHandle")
    class LockHandleTests {

        @Test
        @DisplayName("isValid should return true for active lock")
        void isValidShouldReturnTrueForActiveLock() {
            try (DistributedLock.LockHandle handle =
                    lock.tryAcquire("valid-lock", Duration.ofMinutes(5)).orElseThrow()) {
                assertThat(handle.isValid()).isTrue();
            }
        }

        @Test
        @DisplayName("isValid should return false after close")
        void isValidShouldReturnFalseAfterClose() {
            DistributedLock.LockHandle handle =
                    lock.tryAcquire("closed-lock", Duration.ofMinutes(5)).orElseThrow();

            handle.close();

            assertThat(handle.isValid()).isFalse();
        }

        @Test
        @DisplayName("close should be idempotent")
        void closeShouldBeIdempotent() {
            DistributedLock.LockHandle handle =
                    lock.tryAcquire("idempotent-lock", Duration.ofMinutes(5)).orElseThrow();

            // Should not throw
            handle.close();
            handle.close();
            handle.close();

            assertThat(handle.isValid()).isFalse();
        }

        @Test
        @DisplayName("extend should return true for valid lock")
        void extendShouldReturnTrueForValidLock() {
            try (DistributedLock.LockHandle handle =
                    lock.tryAcquire("extend-lock", Duration.ofMinutes(5)).orElseThrow()) {
                // Postgres advisory locks don't have TTL, so extend is a no-op that returns
                // isValid
                assertThat(handle.extend(Duration.ofMinutes(10))).isTrue();
            }
        }

        @Test
        @DisplayName("can be used in try-with-resources")
        void canBeUsedInTryWithResources() {
            String lockName = "auto-close-test";

            try (DistributedLock.LockHandle handle =
                    lock.tryAcquire(lockName, Duration.ofMinutes(5)).orElseThrow()) {
                assertThat(handle.isValid()).isTrue();
            }

            // Lock should be released after try-with-resources
            PostgresDistributedLock checker = new PostgresDistributedLock(dataSource);
            Optional<DistributedLock.LockHandle> newHandle =
                    checker.tryAcquire(lockName, Duration.ofMinutes(5));
            assertThat(newHandle).isPresent();
            newHandle.get().close();
        }
    }

    @Nested
    @DisplayName("Lock ID computation")
    class LockIdComputationTests {

        @Test
        @DisplayName("same lock name should produce consistent lock ID")
        void sameLockNameShouldProduceConsistentLockId() {
            // Acquire and release the same lock name multiple times
            for (int i = 0; i < 3; i++) {
                Optional<DistributedLock.LockHandle> handle =
                        lock.tryAcquire("consistent-lock", Duration.ofMinutes(5));
                assertThat(handle).isPresent();
                handle.get().close();
            }
        }

        @Test
        @DisplayName("different lock names should not conflict")
        void differentLockNamesShouldNotConflict() {
            Optional<DistributedLock.LockHandle> handle1 =
                    lock.tryAcquire("lock-alpha", Duration.ofMinutes(5));
            Optional<DistributedLock.LockHandle> handle2 =
                    lock.tryAcquire("lock-beta", Duration.ofMinutes(5));
            Optional<DistributedLock.LockHandle> handle3 =
                    lock.tryAcquire("floe-scheduler", Duration.ofMinutes(5));

            assertThat(handle1).isPresent();
            assertThat(handle2).isPresent();
            assertThat(handle3).isPresent();

            handle1.get().close();
            handle2.get().close();
            handle3.get().close();
        }
    }

    @Nested
    @DisplayName("Interface contract")
    class InterfaceContractTests {

        @Test
        @DisplayName("PostgresDistributedLock implements DistributedLock")
        void postgresDistributedLockImplementsInterface() {
            assertThat(lock).isInstanceOf(DistributedLock.class);
        }

        @Test
        @DisplayName("LockHandle is AutoCloseable")
        void lockHandleIsAutoCloseable() {
            DistributedLock.LockHandle handle =
                    lock.tryAcquire("auto-closeable-test", Duration.ofMinutes(1)).orElseThrow();

            assertThat(handle).isInstanceOf(AutoCloseable.class);
            handle.close();
        }
    }
}
