package io.github.dflib.storage.jdbc;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.jdbc.connector.JdbcConnector;
import com.nhl.dflib.jdbc.connector.metadata.DbMetadata;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.dflib.MyJdbcConnector;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.Query;
import io.github.dflib.storage.StorageReader;
import io.github.dflib.storage.config.JdbcStorage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Lazy
@Component
@Slf4j
public class JdbcReader implements StorageReader<JdbcStorage> {

    private final DataSourceFactory jdbcTemplateFactory;

    @Autowired
    public JdbcReader(DataSourceFactory jdbcTemplateFactory) {
        this.jdbcTemplateFactory = jdbcTemplateFactory;
    }

    @Override
    public EventDataFrame read(Query query, JdbcStorage storage) throws StatdException {
        SqlStatement sqlStatement = SqlStatement.create(query, storage);
        log.info("use sql {}, args: {}", sqlStatement.toSql(), sqlStatement.getArguments());
        DataSource dataSource = jdbcTemplateFactory.getDataSource(storage.getDatasource());
        EventDataFrame eventDataFrame = new EventDataFrame();
        DataFrame dataFrame = jdbcConnector(dataSource).sqlLoader(sqlStatement.toSql()).load(sqlStatement.getArguments());
        eventDataFrame.setTable(dataFrame);
        eventDataFrame.setInterval(query.getInterval());
        eventDataFrame.setGranularity(query.getGranularity());
        if (query.isTimeseries()) {
            return eventDataFrame.toZeroDf();
        } else {
            return eventDataFrame;
        }
    }

    private JdbcConnector jdbcConnector(DataSource dataSource) {
        return new MyJdbcConnector(dataSource, DbMetadata.create(dataSource));
    }


}
