# 数据库实现分布式锁

## 1.项目简介
  使用数据库实现分布式锁。

## 2.使用帮助
  * 1.数据库执行 init.sql
  * 2.将 PostgreSQLDistributedLock 拷到自己的项目中
  * 3.使用 PostgreSQLDistributedLock
    ```java
    PostgreSQLDistributedLock lock = new PostgreSQLDistributedLock(dataSource.getConnection(), "test", 30);
    if(lock.tryLock()){
        try {
            // do something
        } finally {
            lock.unlock();
        }
    }
    ```