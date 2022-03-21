package io.github.dflib.dflib;

import com.nhl.dflib.jdbc.connector.DefaultJdbcConnector;
import com.nhl.dflib.jdbc.connector.loader.ColumnBuilder;
import com.nhl.dflib.jdbc.connector.loader.ColumnBuilderFactory;
import com.nhl.dflib.jdbc.connector.metadata.DbMetadata;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class MyJdbcConnector extends DefaultJdbcConnector {
    public MyJdbcConnector(DataSource dataSource,
                           DbMetadata metadata) {
        super(dataSource, metadata, createColumnBuilderFactories());
    }

    @Override
    public ColumnBuilder<?> createColumnReader(int pos, int type, boolean mandatory) {

        //mandatory设置为true代表会默认使用原子类型
        return super.createColumnReader(pos, type, true);
    }

    private static Map<Integer, ColumnBuilderFactory> createColumnBuilderFactories() {
        // add standard factories unless already defined by the user
        Map<Integer, ColumnBuilderFactory> factories = new HashMap<>();

        factories.put(Types.DATE, ColumnBuilderFactory::dateAccum);
        factories.put(Types.TIME, ColumnBuilderFactory::timeAccum);
        factories.put(Types.TIMESTAMP, InstantColumnBuilderFactory::instantAccum);

        return factories;
    }


}
