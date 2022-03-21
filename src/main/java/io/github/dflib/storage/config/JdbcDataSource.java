package io.github.dflib.storage.config;

import io.github.dflib.exception.StatdException;
import lombok.Data;

import java.util.Objects;

@Data
public class JdbcDataSource implements Datasource {
    private String url;
    private String user;
    private String password;
    private String driverClass;

    @Override
    public void validate() throws StatdException {
        Objects.requireNonNull(url, "jdbc url not provided");
    }
}