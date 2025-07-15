package com.example;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.concurrent.*;

public class PostgreSQLDistributedLock {
    private final Connection connection;
    private final String lockKey;
    private final String lockHolder;
    private final long lockExpireSeconds;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> renewalTask;
    private volatile boolean isLocked = false;

    public PostgreSQLDistributedLock(Connection connection, String lockKey, long lockExpireSeconds) {
        this.connection = connection;
        this.lockKey = lockKey;
        try {
            this.lockHolder = InetAddress.getLocalHost().getHostAddress() + ":" + Thread.currentThread().getId();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        this.lockExpireSeconds = lockExpireSeconds;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public boolean tryLock() {
        if (isLocked) {
            return true;
        }
        String sql = "INSERT INTO distributed_lock (lock_key, lock_holder, expire_time) VALUES (?, ?, NOW() + (? || ' seconds')::INTERVAL ) " +
                "ON CONFLICT (lock_key) DO UPDATE SET lock_holder = EXCLUDED.lock_holder, expire_time = EXCLUDED.expire_time " +
                "WHERE distributed_lock.expire_time <= NOW()";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, lockKey);
            stmt.setString(2, lockHolder);
            stmt.setLong(3, lockExpireSeconds);
            int count = stmt.executeUpdate();
            if (count > 0) {
                isLocked = true;
                startRenewalTask();  // 启动续期任务
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            if (tryLock()) {
                return true;
            }
            if (System.nanoTime() >= endTime) {
                return false;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }

    public void unlock() {
        if (!isLocked) {
            return;
        }
        if (renewalTask != null) {
            renewalTask.cancel(false);
        }
        String sql = "DELETE FROM distributed_lock WHERE lock_key = ? AND lock_holder = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, lockKey);
            stmt.setString(2, lockHolder);
            stmt.executeUpdate();
            isLocked = false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isLocked() {
        return isLocked;
    }

    private void startRenewalTask() {
        renewalTask = scheduler.scheduleAtFixedRate(() -> {
            try {
                String sql = "UPDATE distributed_lock SET expire_time = NOW() + (? || ' seconds')::INTERVAL WHERE lock_key = ? AND lock_holder = ?";
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setLong(1, lockExpireSeconds);
                    stmt.setString(2, lockKey);
                    stmt.setString(3, lockHolder);
                    int updated = stmt.executeUpdate();
                    if (updated == 0) {
                        isLocked = false;
                        renewalTask.cancel(false);  // 续期失败，锁已丢失
                    }
                }
            } catch (SQLException e) {
                isLocked = false;
                renewalTask.cancel(false);
            }
        }, lockExpireSeconds / 3, lockExpireSeconds / 3, TimeUnit.SECONDS);  // 每 1/3 锁时间续期一次
    }
}
