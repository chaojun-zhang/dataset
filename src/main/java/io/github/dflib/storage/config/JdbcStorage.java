package io.github.dflib.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.dflib.query.Granularity;
import io.github.dflib.storage.jdbc.JdbcReader;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JdbcStorage implements SourceStorage {

    //以下三个不能为空
    private String datasource;
    private String eventTimeField;
    private Granularity granularity;

    private JdbcStorageType storageType;
    private String table;
    private String sql;
    private int zoneOffsetHour;

    public boolean isSqlNotEmpty() {
        return StringUtils.isNotEmpty(sql);
    }

    @JsonIgnore
    @Override
    public Class<JdbcReader> getReaderClass() {
        return JdbcReader.class;
    }

    @Override
    public void validate() {
        Objects.requireNonNull(datasource, "datasource not provided");
        Objects.requireNonNull(eventTimeField, "eventTimeField not provided");
        Objects.requireNonNull(granularity, "granularity not provided");
        if (sql == null) {
            Objects.requireNonNull(table, "table not provided");
            Objects.requireNonNull(storageType, "storageType not provided");
            Objects.requireNonNull(granularity, "granularity not provided");
            if (this.getGranularity() == Granularity.GAll) {
                List<String> avaliableGranularity = Arrays.stream(Granularity.values()).filter(it -> it != Granularity.GAll).map(Object::toString).collect(Collectors.toList());
                throw new IllegalStateException("granularity can only be one of: " + String.join(",", avaliableGranularity));
            }
        }
    }

}