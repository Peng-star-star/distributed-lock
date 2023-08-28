create table if not exists lock_noblock(
lock_name character varying(255) primary key,
state integer,
expire_time timestamp
);
create table if not exists lock_block(
lock_name character varying(255) primary key
);