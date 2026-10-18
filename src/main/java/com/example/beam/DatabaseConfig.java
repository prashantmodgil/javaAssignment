package com.example.beam;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

/** Holds JDBC connection settings and adapts them to Beam's JDBC sink. */
public final class DatabaseConfig {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    private DatabaseConfig(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static DatabaseConfig from(GcsToPostgresOptions options) {
        return new DatabaseConfig(
                options.getJdbcUrl(),
                options.getDbUsername(),
                options.getDbPassword());
    }

    public JdbcIO.DataSourceConfiguration toDataSourceConfiguration() {
        return JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", jdbcUrl)
                .withUsername(username)
                .withPassword(password);
    }
}
