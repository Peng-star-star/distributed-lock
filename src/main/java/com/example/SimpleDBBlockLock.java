package com.example;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.concurrent.locks.AbstractOwnableSynchronizer;

/**
 * 简单的通过DB(PostgreSQL)实现的阻塞的分布式锁，它可以在生产环境中使用，但要注意阻塞时会占用数据连接池资源。它具有以下功能：<br/>
 * <pre>
 *   lock
 *     有阻塞获取锁
 *     可重入
 *   unlock
 *   无死锁，当获取到分布式锁的进程死掉，需要提供释放锁的机制，否则锁将成死锁。
 * </pre>
 * 它还未有以下功能：<br/>
 * <pre>
 *   tryLock
 *   lockInterruptibly
 *   tryLock(time)
 *   newCondition
 * </pre>
 */
public class SimpleDBBlockLock extends AbstractOwnableSynchronizer {

    /**
     * 不存在就插入；存在就什么也不做
     */
    private static final String insertSQL = "insert into lock_block(lock_name) values(?) on conflict do nothing";
    private static final String selectForUpdateSQL = "select * from lock_block where lock_name = ? for update";

    private DataSource ds;
    private Connection exclusiveOwnerConn;
    private String lockName;
    private volatile int state;

    public SimpleDBBlockLock(String lockName, DataSource ds) {
        this.lockName = lockName;
        this.ds = ds;
        initData();
    }

    public void lock() {
        if (isHeldByCurrentThread()) {
            // 如果能执行到这儿，说明一定是单线程执行，state的设置不是原子也没有关系
            int nextc = getState() + 1;
            if (nextc < 0) {// overflow
                throw new Error("Maximum lock count exceeded");
            }
            setState(nextc);
        } else {
            blockGetLock();
        }
    }

    public void unlock() {
        if (!isHeldByCurrentThread()) {
            throw new IllegalMonitorStateException();
        } else {
            int c = getState() - 1;
            if (c > 0) {
                // 这是设置可重入数，还没有释放锁
                setState(c);
                return;
            }
            // 重入次数为0，解锁
            setExclusiveOwnerThread(null);
            setState(c);
            Connection conn = exclusiveOwnerConn;
            try {
                exclusiveOwnerConn = null;
                conn.commit();
                // 还原不自动提交
                conn.setAutoCommit(false);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                close(conn);
            }
        }
    }

    private int getState() {
        return state;
    }

    private void setState(int state) {
        this.state = state;
    }

    private void initData() {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(insertSQL);
            ps.setString(1, lockName);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("无法创建[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(conn);
        }
    }

    private void blockGetLock() {
        // 1.select for update
        Connection conn = getConn();
        selectForUpdate(conn);
        // 2.set state
        setState(1);
        // 3.set thread
        setExclusiveOwnerThread(Thread.currentThread());
        // 4.set connection
        this.exclusiveOwnerConn = conn;
    }

    private void selectForUpdate(Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(selectForUpdateSQL);
            ps.setString(1, lockName);
            ps.setQueryTimeout(0);
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException(MessageFormat.format("无法获取锁，未在数据库找到[{0}]锁", lockName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("无法获取[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(rs);
        }
    }

    private boolean isHeldByCurrentThread() {
        return Thread.currentThread() == getExclusiveOwnerThread();
    }

    private void close(AutoCloseable ac) {
        try {
            if (ac != null) {
                ac.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConn() {
        Connection conn = null;
        try {
            conn = ds.getConnection();
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
