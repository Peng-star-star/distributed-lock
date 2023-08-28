package com.example;

import javax.sql.DataSource;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link SimpleDBNoBlockLock SimpleDBNoBlockLock} 改进型，由单锁改为两级锁。在高并发情况下减少对数据库的访问。</br>
 * 简单的通过DB(PostgreSQL)实现的无阻塞的分布式锁。它具有以下功能：<br/>
 * <pre>
 *   tryLock
 *     无阻塞互斥锁
 *     可重入
 *   unlock
 *   无死锁，当获取到分布式锁的进程死掉，需要提供释放锁的机制，否则锁将成死锁。
 * </pre>
 * 它还未有以下功能：<br/>
 * <pre>
 *   lock
 *   lockInterruptibly
 *   tryLock(long leaseTime, TimeUnit unit)
 *   tryLock(time)
 *   newCondition
 * </pre>
 *
 */
public class SimpleDBNoBlockCompositeLock {

    private ReentrantLock lockWithinApp;
    private SimpleDBNoBlockLock distributedLock;

    public SimpleDBNoBlockCompositeLock(String lockName, DataSource ds) {
        lockWithinApp = new ReentrantLock();
        distributedLock = new SimpleDBNoBlockLock(lockName, ds);
    }

    public boolean tryLock() {
        if (lockWithinApp.tryLock()) {
            return distributedLock.tryLock();
        }
        return false;
    }

    public void unlock() {
        distributedLock.unlock();
        lockWithinApp.unlock();
    }
}
