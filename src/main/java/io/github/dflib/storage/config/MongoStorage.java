package io.github.dflib.storage.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.Granularity;
import io.github.dflib.storage.StorageReader;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MongoStorage implements SourceStorage {
    private String datasource;
    private String database;
    private String collection;
    private String eventTimeField;
    //时间是否采取压缩存放
    private boolean compact;
    private Granularity granularity;
    private String rowGranularity;

    @Override
    public Class<StorageReader> getReaderClass() {
        return null;
    }

    @Override
    public void validate() throws StatdException {
        Objects.requireNonNull(datasource, "datasource not provided");
        Objects.requireNonNull(database, "datasource not provided");
        Objects.requireNonNull(collection, "collection not provided");
        Objects.requireNonNull(eventTimeField, "eventTimeField not provided");
        Objects.requireNonNull(granularity, "granularity not provided");

        if (this.getGranularity() == Granularity.GAll) {
            List<String> avaliableGranularity = Arrays.stream(Granularity.values()).filter(it -> it != Granularity.GAll).map(Object::toString).collect(Collectors.toList());
            throw new IllegalStateException("granularity can only be one of: " + String.join(",", avaliableGranularity));
        }

    }

}
