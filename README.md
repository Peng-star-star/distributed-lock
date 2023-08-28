# 数据库实现分布式锁

## 1.项目简介
  使用 PostgreSQL 实现分布式锁。

## 2.使用帮助
  * 1.数据库执行 init.sql
  * 2.将 SimpleDBNoBlockLock 拷到自己的项目中
  * 3.引入 PostgreSQl 和 Druid 包
    ```java
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>42.2.5</version>
    </dependency>
    <dependency>
        <groupId>com.alibaba</groupId>
        <artifactId>druid</artifactId>
        <version>1.1.9</version>
    </dependency>
    ```
  * 4.使用 SimpleDBNoBlockLock
    ```java
    // 创建数据库连接池
    DruidDataSource ds = new DruidDataSource();
    ds.setDriverClassName("org.postgresql.Driver");
    ds.setUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
    ds.setUsername("postgres");
    ds.setPassword("admin");
    ds.setRemoveAbandoned(false);
    ds.setMaxActive(20);
    // 使用分布式锁
    SimpleDBNoBlockLock lock = new SimpleDBNoBlockLock("mylock", ds);
    if(lock.tryLock()){
        try {
            // do something
        } finally {
            lock.unlock();
        }
    }
    ```
## 3.分布式锁选择

|  功能   |SimpleDBNoBlockLock|SimpleDBBlockLock|SimpleDBLock|
|  ----   |----  |----  |----  |
|  1.void lock()   |N|Y|Y|
|  2.void lockInterruptibly() throws InterruptedException; |N|N|N|
|  3.boolean tryLock();   |Y|N|Y|
|  4.boolean tryLock(long time, TimeUnit unit) throws InterruptedException |N|N|N|
|  5.boolean tryLock(long leaseTime, TimeUnit unit)   |Y|N|N|
|  6.void unlock(); |Y|Y|Y|
|  7.Condition newCondition(); |N|N|N|
|  8.无死锁 |Y|Y|Y|

建议使用 SimpleDBNoBlockLock ，它在获取锁期间不会一直占用数据库连接池的连接。而 SimpleDBBlockLock 与 SimpleDBLock 存在数据库连接池耗尽风险。