package com.example;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

public class DataSourceHelp {

    public static DataSource getDataSource(){
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        ds.setUsername("postgres");
        ds.setRemoveAbandoned(false);
        ds.setMaxActive(20);
        return ds;
    }
}
