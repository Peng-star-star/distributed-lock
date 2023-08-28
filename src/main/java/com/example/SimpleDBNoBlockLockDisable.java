package com.example;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;

/**
 * 简单的通过DB(PostgreSQL)实现的无阻塞的分布式锁，它是用来学习编写分布式锁的，不具备在生产环境中使用。它具有以下功能：<br/>
 * <pre>
 *   tryLock
 *     无阻塞获取锁
 *     可重入
 *   unlock
 * </pre>
 * 它还未有以下功能：<br/>
 * <pre>
 *   无死锁，当获取到分布式锁的进程死掉，需要提供释放锁的机制，否则锁将成死锁。
 *   lock
 *   lockInterruptibly
 *   tryLock(time)
 *   newCondition
 * </pre>
 * 还存在以下问题：<br/>
 * <pre>
 *   重入特别耗时，特别在测试最大重入数时
 * </pre>
 */
public class SimpleDBNoBlockLockDisable {

    /**
     * 不存在就插入；存在就什么也不做
     */
    private static final String insertSQL = "insert into lock_noblock(lock_name,state) values(?,?) on conflict do nothing";
    private static final String compareAndSwapSQL = "update lock_noblock set  state = ?  where lock_name = ? and state = ?";
    private static final String selectSQL = "select * from lock_noblock t where t.lock_name = ?";
    private static final String updateStateSQL = "update lock_noblock set state = ? where lock_name = ?";
    private static final String updateThreadSQL = "update lock_noblock set thread_id = ? where lock_name = ? ";
    private DataSource ds;
    private String lockName;
    private ThreadLocal<Connection> threadLocal;

    public SimpleDBNoBlockLockDisable(String lockName, DataSource ds) {
        this.lockName = lockName;
        this.ds = ds;
        this.threadLocal = new ThreadLocal<>();
        initData();
    }

    public boolean tryLock() {
        return tryLock(1);
    }

    public void unlock() {
        unlock(1);
    }

    private boolean tryLock(int acquires) {
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            threadLocal.set(conn);
            int state = getState();
            boolean flag = false;
            if (state == 0) {
                if (compareAndSetState(0, acquires)) {
                    setExclusiveOwnerThread(Thread.currentThread());
                    flag = true;
                }
            } else if (isHeldByCurrentThread()) {
                int nextc = state + acquires;
                if (nextc < 0) {// overflow
                    throw new Error("Maximum lock count exceeded");
                }
                setState(nextc);
                flag = true;
            }
            threadLocal.remove();
            conn.commit();
            conn.setAutoCommit(true);
            return flag;
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("无法创建[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(conn);
        }
    }

    private boolean unlock(int releases) {
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            threadLocal.set(conn);
            int c = getState() - releases;
            if (!isHeldByCurrentThread()) {
                throw new IllegalMonitorStateException();
            }
            boolean free = false;
            if (c == 0) {
                free = true;
                setExclusiveOwnerThread(null);
            }
            setState(c);
            threadLocal.remove();
            conn.commit();
            conn.setAutoCommit(true);
            return free;
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("无法释放[{0}]锁", lockName), e);
        } finally {
            close(rs);
            close(ps);
            close(conn);
        }
    }

    private void initData() {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(insertSQL);
            ps.setString(1, lockName);
            ps.setInt(2, 0);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("无法创建[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(conn);
        }
    }

    private int getState() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = threadLocal.get();
            ps = conn.prepareStatement(selectSQL);
            ps.setString(1, lockName);
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException(MessageFormat.format("无法找到[{1}]锁", lockName));
            }
            return rs.getInt("state");
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(rs);
        }
    }

    private void setState(int state) {
        PreparedStatement ps = null;
        try {
            Connection conn = threadLocal.get();
            ps = conn.prepareStatement(updateStateSQL);
            ps.setInt(1, state);
            ps.setString(2, lockName);
            int count = ps.executeUpdate();
            if (count == 0) {
                throw new RuntimeException(MessageFormat.format("无法找到[{1}]锁", lockName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
        }
    }

    private boolean compareAndSetState(int expect, int update) {
        PreparedStatement ps = null;
        try {
            Connection conn = threadLocal.get();
            ps = conn.prepareStatement(compareAndSwapSQL);
            ps.setInt(1, update);
            ps.setString(2, lockName);
            ps.setInt(3, expect);
            return ps.executeUpdate() == 1 ? true : false;
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
        }
    }

    private boolean isHeldByCurrentThread() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = threadLocal.get();
            ps = conn.prepareStatement(selectSQL);
            ps.setString(1, lockName);
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException(MessageFormat.format("无法找到[{1}]锁", lockName));
            }
            return getThreadId(Thread.currentThread()).equals(rs.getString("thread_id"));
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(rs);
        }
    }

    private void setExclusiveOwnerThread(Thread thread) {
        PreparedStatement ps = null;
        try {
            Connection conn = threadLocal.get();
            ps = conn.prepareStatement(updateThreadSQL);
            ps.setString(1, getThreadId(thread));
            ps.setString(2, lockName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
        }
    }

    private String getThreadId(Thread thread) {
        return thread==null?null:String.valueOf(thread.getId());
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
}
