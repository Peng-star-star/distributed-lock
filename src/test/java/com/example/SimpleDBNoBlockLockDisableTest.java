package com.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class SimpleDBNoBlockLockDisableTest {

    SimpleDBNoBlockLockDisable lock = new SimpleDBNoBlockLockDisable("mylock", DataSourceHelp.getDataSource());

    @Test
    public void testTryLockUnlock() {
        lock.tryLock();
        lock.unlock();

        lock.tryLock();
        lock.unlock();
    }


    @Test
    public void testReentrant() throws InterruptedException {
        lock.tryLock();
        lock.tryLock();
        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testOtherThreadUnlock() {
        lock.tryLock();
        try {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    boolean unlock = true;
                    try {
                        lock.unlock();
                    } catch (IllegalMonitorStateException e) {
                        unlock = false;
                    }
                    Assertions.assertFalse(unlock);
                }
            });
            thread.start();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 测试可重入的最大次数为 Integer.MAX_VALUE
     */
    @Test
    public void testReentrantOverMaximum() {
        // 可重入执行时间太长
        Assertions.assertTrue(false);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            lock.tryLock();
        }
        boolean test = false;
        try {
            lock.tryLock();
        } catch (Error e) {
            test = "Maximum lock count exceeded".equals(e.getMessage());
        }
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            lock.unlock();
        }
        Assertions.assertTrue(test);
    }

    /**
     * 测试可重入的unlock必须与lock次数一样锁才释放
     *
     * @throws InterruptedException
     */
    @Test
    public void testReentrantUnlockMustMatchLockCount() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Assertions.assertFalse(lock.tryLock());
            }
        });
        thread.start();
        Assertions.assertTrue(lock.tryLock());
        countDownLatch.countDown();
        Assertions.assertTrue(lock.tryLock());
        lock.unlock();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
