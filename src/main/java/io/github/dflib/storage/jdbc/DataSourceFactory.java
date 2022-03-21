package io.github.dflib.storage.jdbc;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.github.dflib.storage.config.Config;
import io.github.dflib.storage.config.Datasource;
import io.github.dflib.storage.config.JdbcDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Objects;

@Component
public class DataSourceFactory {

    private final Config config;

    private final LoadingCache<JdbcDataSource, DataSource> dataSources;

    @Autowired
    public DataSourceFactory(Config config) {
        this.config = config;
        this.dataSources = Caffeine.newBuilder().build(this::loadDataSource);
    }

    public JdbcTemplate getJdbcTemplate(String dataSource) {
        return new JdbcTemplate(getDataSource(dataSource));
    }

    public DataSource getDataSource(String dataSource) {
        Objects.requireNonNull(dataSource);
        Datasource datasource = config.getDatasource().get(dataSource);
        if (!(datasource instanceof JdbcDataSource)) {
            throw new IllegalArgumentException(String.format("datasource '%s' not defined", dataSource));
        }
        JdbcDataSource jdbcDataSource = (JdbcDataSource) datasource;
        return dataSources.get(jdbcDataSource);
    }

    private DataSource loadDataSource(JdbcDataSource jdbcDataSource) {
        if (jdbcDataSource.getUser() != null) {
            return DataSourceBuilder.create()
                    .url(jdbcDataSource.getUrl())
                    .username(jdbcDataSource.getUser())
                    .password(jdbcDataSource.getPassword())
                    .driverClassName(jdbcDataSource.getDriverClass())
                    .build();

        } else {
            return DataSourceBuilder.create()
                    .url(jdbcDataSource.getUrl())
                    .driverClassName(jdbcDataSource.getDriverClass())
                    .build();
        }
    }
}
