package com.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleDBBlockLockTest {

    SimpleDBBlockLock lock = new SimpleDBBlockLock("mylock", DataSourceHelp.getDataSource());
    ExecutorService executorService = Executors.newFixedThreadPool(5);

    /**
     * 测试加锁解锁
     */
    @Test
    public void testLockUnlock() {
        lock.lock();
        lock.unlock();
        Assertions.assertTrue(true);
    }

    /**
     * 测试有阻塞互斥锁
     */
    @Test
    public void testLock() {
        long block = 2000L; // 2s
        boolean[] flag = {false};
        try {
            lock.lock();
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    try {
                        lock.lock();
                        flag[0] = (System.currentTimeMillis() - startTime) > block;
                    } finally {
                        lock.unlock();
                    }
                }
            });
            Thread.sleep(block);
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        } finally {
            lock.unlock();
        }
        // 等待线程池执行完
        executorService.shutdown();
        try {
            executorService.awaitTermination(Integer.MAX_VALUE,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        }
        Assertions.assertTrue(flag[0]);
    }

    /**
     * 测试可重入
     */
    @Test
    public void testReentrant() {
        lock.lock();
        lock.lock();
        lock.unlock();
        lock.unlock();
    }

    /**
     * 测试无死锁
     */
    @Test
    public void testNoDeadlock() {
        Assertions.assertTrue(false);
    }


    /**
     * 测试非加锁线程进行解锁
     */
    @Test
    public void testOtherThreadUnlock() {
        final boolean[] flag = {false};
        try {
            lock.lock();
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        lock.unlock();
                    } catch (IllegalMonitorStateException e) {
                        flag[0] = true;
                    }
                }
            });
            executorService.shutdown();
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Assertions.assertTrue(false);
            }
        } finally {
            lock.unlock();
        }
        Assertions.assertTrue(flag[0]);
    }

    /**
     * 测试可重入的最大次数为 Integer.MAX_VALUE
     */
    @Test
    public void testReentrantOverMaximum() {
        Assertions.assertTrue(false);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            lock.lock();
        }
        boolean test = false;
        try {
            lock.lock();
        } catch (Error e) {
            Assertions.assertEquals("Maximum lock count exceeded", e.getMessage());
        } finally {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                lock.unlock();
            }
        }
    }

    /**
     * 测试可重入的unlock必须与lock次数一样锁才释放
     *
     * @throws InterruptedException
     */
    @Test
    public void testReentrantUnlockMustMatchLockCount() {
        long block = 2000L; // 2s
        boolean[] flag = {false};
        try {
            lock.lock();
            lock.lock();
            lock.unlock();
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    try {
                        lock.lock();
                        flag[0] = (System.currentTimeMillis() - startTime) > block;
                    } finally {
                        lock.unlock();
                    }
                }
            });
            Thread.sleep(block);
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        } finally {
            lock.unlock();
        }
        // 等待线程池执行完
        executorService.shutdown();
        try {
            executorService.awaitTermination(Integer.MAX_VALUE,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        }
        Assertions.assertTrue(flag[0]);
    }
}
