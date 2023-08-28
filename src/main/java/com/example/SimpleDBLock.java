package com.example;

import javax.sql.DataSource;

/**
 * 简单的通过DB(PostgreSQL)实现的分布式锁。它具有以下功能：<br/>
 * <pre>
 *   lock
 *     有阻塞获取锁
 *     可重入
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
 *   tryLock(time)
 *   newCondition
 * </pre>
 */
public class SimpleDBLock {

    private SimpleDBBlockLock blockLock;
    private SimpleDBNoBlockLock noBlockLock;

    public SimpleDBLock(String lockName, DataSource ds) {
        blockLock = new SimpleDBBlockLock(lockName, ds);
        noBlockLock = new SimpleDBNoBlockLock(lockName, ds);
    }

    public void lock() {
        while (true) {
            if (noBlockLock.tryLock()) {
                blockLock.lock();
                return;
            } else {
                // 仅仅使用blockLock的阻塞功能，所以后面一定要跟解锁。
                blockLock.lock();
                blockLock.unlock();
            }
        }
    }

    public boolean tryLock() {
        if (noBlockLock.tryLock()) {
            blockLock.lock();
            return true;
        } else {
            return false;
        }
    }

    public void unlock() {
        blockLock.unlock();
        noBlockLock.unlock();
    }
}