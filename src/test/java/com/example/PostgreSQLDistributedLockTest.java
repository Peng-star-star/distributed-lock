package com.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PostgreSQLDistributedLockTest {

    private final DataSource dataSource;

    public PostgreSQLDistributedLockTest() {
        dataSource = DataSourceHelp.getDataSource();
    }

    @BeforeEach
    void cleanup() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM distributed_lock");
        }
    }

    // 测试lock
    @Test
    public void testTryLockSuccess() throws SQLException {
        PostgreSQLDistributedLock lock = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        Assertions.assertTrue(lock.tryLock());
        Assertions.assertTrue(lock.isLocked());
        lock.unlock();
    }

    @Test
    void testTryLockFailWhenLocked() throws SQLException {
        PostgreSQLDistributedLock lock1 = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        PostgreSQLDistributedLock lock2 = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        Assertions.assertTrue(lock1.tryLock());
        Assertions.assertFalse(lock2.tryLock());
        lock1.unlock();
        Assertions.assertTrue(lock2.tryLock());
        lock2.unlock();
    }

    @Test
    void testTryLockWithTimeout() throws InterruptedException, SQLException {
        PostgreSQLDistributedLock lock1 = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        PostgreSQLDistributedLock lock2 = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        Assertions.assertTrue(lock1.tryLock());
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                lock1.unlock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
        Assertions.assertTrue(lock2.tryLock(2, TimeUnit.SECONDS));
        lock2.unlock();
    }

    @Test
    void testAutoRenewal() throws InterruptedException, SQLException {
        PostgreSQLDistributedLock lock = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        Assertions.assertTrue(lock.tryLock());
        Thread.sleep(1500);
        Assertions.assertTrue(lock.isLocked());
        lock.unlock();
    }

    @Test
    void testConcurrentAccess() throws InterruptedException {
        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                PostgreSQLDistributedLock lock = null;
                try {
                    lock = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                try {
                    if (lock.tryLock()) {
                        successCount.incrementAndGet();
                        Thread.sleep(500);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                    latch.countDown();
                }
            });
        }
        latch.await();
        Assertions.assertEquals(1, successCount.get());
    }

    @Test
    void testUnlockWhenNotHeld() throws SQLException {
        PostgreSQLDistributedLock lock = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
        Assertions.assertDoesNotThrow(lock::unlock);
    }
}
