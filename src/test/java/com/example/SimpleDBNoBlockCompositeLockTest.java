package com.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleDBNoBlockCompositeLockTest {

    SimpleDBNoBlockCompositeLock lock = new SimpleDBNoBlockCompositeLock("mylock", DataSourceHelp.getDataSource());
    ExecutorService executorService = Executors.newFixedThreadPool(5);

    /**
     * 测试加锁解锁，tryLock方法
     */
    @Test
    public void testTryLockUnlock() {
        Assertions.assertTrue(lock.tryLock());
        lock.unlock();
    }

    /**
     * 测试无阻塞互斥锁
     */
    @Test
    public void testTryLock() {
        boolean[] flags = {false, false, false};
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                String name = Thread.currentThread().getName();
                int index = Integer.valueOf(name.substring(name.lastIndexOf("-") + 1)) - 1;
                flags[index] = lock.tryLock() == false;
            }
        };
        Assertions.assertTrue(lock.tryLock());
        executorService.execute(runnable);
        executorService.execute(runnable);
        executorService.execute(runnable);
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            Assertions.assertArrayEquals(new boolean[]{true, true, true}, flags);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 测试可重入，tryLock方法
     */
    @Test
    public void testReentrant() {
        Assertions.assertTrue(lock.tryLock());
        Assertions.assertTrue(lock.tryLock());
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
        boolean[] flags = {false};
        if (lock.tryLock()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        lock.unlock();
                    } catch (IllegalMonitorStateException e) {
                        flags[0] = true;
                    }
                }
            });
            try {
                executorService.shutdown();
                executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                Assertions.assertTrue(flags[0]);
            } catch (InterruptedException e) {
                Assertions.assertTrue(false);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 测试可重入的最大次数为 Integer.MAX_VALUE
     */
    @Test
    public void testReentrantOverMaximum() {
        Assertions.assertTrue(false);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            lock.tryLock();
        }
        boolean test = false;
        try {
            lock.tryLock();
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
        boolean[] flags = {false};
        Assertions.assertTrue(lock.tryLock());
        Assertions.assertTrue(lock.tryLock());
        lock.unlock();
        try {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    flags[0] = lock.tryLock() == false;
                }
            });
            // 等待线程池执行完
            executorService.shutdown();
            try {
                executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Assertions.assertTrue(false);
            }
            Assertions.assertTrue(flags[0]);
        } finally {
            lock.unlock();
        }
    }
}
