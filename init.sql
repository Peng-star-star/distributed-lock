CREATE TABLE distributed_lock
(
    lock_key    VARCHAR(255) PRIMARY KEY, -- 锁的唯一标识（如业务ID）
    lock_holder VARCHAR(255),             -- 锁持有者（如机器IP+线程ID）
    expire_time TIMESTAMP                 -- 锁过期时间
);