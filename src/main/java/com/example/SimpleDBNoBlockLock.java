package com.example;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;


/**
 * 简单的通过DB(PostgreSQL)实现的无阻塞的分布式锁。它具有以下功能：<br/>
 * <pre>
 *   tryLock
 *     无阻塞互斥锁
 *     可重入
 *   tryLock(long leaseTime, TimeUnit unit)
 *     无阻塞互斥锁
 *     可重入
 *     指定锁的过期时间
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
 * 解锁有2种情况：
 * <pre>
 *   1.unlock
 *   2.锁超时，自动解锁。锁超时有两种情况：1.执行有限制时间的tryLock，锁超时。2.进程死掉，锁超时。
 * </pre>
 */
public class SimpleDBNoBlockLock {

    /**
     * 初始化数据，state只能为0或者1；0表示锁是空闲的，线程可以获取锁；1表示锁被占用。
     */
    private static final String insertSQL = "insert into lock_noblock(lock_name,state) values(?,?) on conflict do nothing";
    private static final String compareAndSwapSetExpireSQL = "update lock_noblock set state = ? ,expire_time=LOCALTIMESTAMP + interval {0} where lock_name = ? and ( state = ? or expire_time < LOCALTIMESTAMP)";
    private static final String updateExpireSQL = "update lock_noblock set expire_time = expire_time + interval {0} where lock_name = ?";
    private static final String getExpireSQL = "select EXTRACT(epoch FROM (expire_time-LOCALTIMESTAMP))*1000 from lock_noblock where lock_name = ?";

    /**
     * 参考 {@link java.util.concurrent.locks.AbstractQueuedSynchronizer#spinForTimeoutThreshold spinForTimeoutThreshold}
     */
    static final long spinForTimeoutThreshold = 1000L;
    /**
     * 默认过期时间 30s
     */
    static final long defaultExpire = 30000L;
    /**
     * 锁续期间隔时间 </br>
     * expire <= 0 锁已过期，不能再续期
     * expire <= renewalInterval 立即执行续期
     * expire > renewalInterval sleep 到 renewalInterval 执行续期
     */
    static final long renewalInterval = 10000L;

    /**
     * 仅仅在可重入使用，表示重入的次数，不可以通过state=0判断锁已经释放
     */
    private int state;
    private DataSource ds;
    private String lockName;
    private Thread thread;
    /**
     * 必须写 volatile，不能使用{@link java.util.concurrent.locks.AbstractOwnableSynchronizer}
     */
    private volatile Thread exclusiveOwnerThread;

    public SimpleDBNoBlockLock(String lockName, DataSource ds) {
        this.lockName = lockName;
        this.ds = ds;
        initData();
    }

    public boolean tryLock() {
        return tryGetLock(-1, TimeUnit.SECONDS);
    }

    public boolean tryLock(long leaseTime, TimeUnit unit) {
        return tryGetLock(leaseTime, unit);
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
            setState(c);
            setExclusiveOwnerThread(null);
            if (thread != null) {
                thread.interrupt();
            }
            // 这儿原本可以直接更新state=0，这样就等于释放锁了，为了少些代码直接使用cas更新
            compareAndSetStateAndSetExpire(1, 0, 0);
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
        return state;
    }

    private void setState(int state) {
        this.state = state;
    }

    private boolean tryGetLock(long leaseTime, TimeUnit unit) {
        long expire = 0;
        try {
            expire = getExpire();
        } catch (InterruptedException e) {
            return false;
        }
        if (expire <= 0) {
            setState(0);
            setExclusiveOwnerThread(null);
        }
        if (!isHeldByCurrentThread()) {
            if (compareAndSetStateAndSetExpire(0, 1, leaseTime == -1 ? defaultExpire : unit.toMillis(leaseTime))) {
                setState(1);
                setExclusiveOwnerThread(Thread.currentThread());
                if (leaseTime == -1) {
                    renewal(defaultExpire);
                }
                return true;
            }
        } else {
            int nextc = getState() + 1;
            if (nextc < 0) {// overflow
                throw new Error("Maximum lock count exceeded");
            }
            setState(nextc);
            return true;
        }
        return false;
    }

    private boolean compareAndSetStateAndSetExpire(int expect, int update, long expire) {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(setExpireSQL(compareAndSwapSetExpireSQL, expire));
            ps.setInt(1, update);
            ps.setString(2, lockName);
            ps.setInt(3, expect);
            return ps.executeUpdate() == 1 ? true : false;
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(conn);
        }
    }

    private void renewal(long millisecond) {
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    long expire = 0;
                    try {
                        expire = getExpire();
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (expire <= 0) {
                        throw new RuntimeException("锁已过期");
                    } else if (expire <= renewalInterval) {
                        doRenewal(millisecond);
                    } else {
                        try {
                            Thread.sleep(expire - renewalInterval);
                        } catch (InterruptedException e) {
                            break;
                        }
                        doRenewal(millisecond);
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private boolean doRenewal(long millisecond) {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            String data = new String("'" + millisecond + " millisecond'");
            ps = conn.prepareStatement(MessageFormat.format(updateExpireSQL, data));
            ps.setString(1, lockName);
            return ps.executeUpdate() == 1 ? true : false;
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("找到[{0}]锁", lockName), e);
        } finally {
            close(ps);
            close(conn);
        }
    }

    private long getExpire() throws InterruptedException {
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(getExpireSQL);
            ps.setString(1, lockName);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new RuntimeException(MessageFormat.format("未找到[{0}]锁", lockName));
        } catch (SQLException e) {
            if(e.getCause() instanceof InterruptedException){
                throw new InterruptedException("执行获取过期时间时，执行了中断操作。");
            }else{
                throw new RuntimeException(e);
            }
        } finally {
            close(rs);
            close(ps);
            close(conn);
        }
    }

    private String setExpireSQL(String sql, long expire) {
        String expireStr = new String("'" + expire + " millisecond'");
        return MessageFormat.format(sql, expireStr);
    }

    /**
     * 锁是否由当前线程持有 </br>
     * ReentrantLock 不需要设置 exclusiveOwnerThread 为 volatile。因为只能一个线程能设置，并且A线程设置exclusiveOwnerThread，B线程不管是否能取到这个设置的值都是false。</br>
     *
     * @return true锁由当前线程持有；false锁不为当前线程持有。
     */
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

    private void setExclusiveOwnerThread(Thread thread) {
        exclusiveOwnerThread = thread;
    }

    private Thread getExclusiveOwnerThread() {
        return exclusiveOwnerThread;
    }

}
