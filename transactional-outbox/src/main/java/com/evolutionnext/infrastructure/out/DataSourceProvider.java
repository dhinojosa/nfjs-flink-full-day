package com.evolutionnext.infrastructure.out;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

public class DataSourceProvider {
    public static DataSource create() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL("jdbc:postgresql://localhost:5432/orders");
        ds.setUser("postgres");
        ds.setPassword("postgres");
        return ds;
    }
}
