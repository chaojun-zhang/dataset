package io.github.dflib.storage;

import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.Query;
import io.github.dflib.storage.config.Storage;

import java.util.Map;

public interface StorageReader<T extends Storage> {

    EventDataFrame read(Query query, T storage) throws StatdException;

    default Map<String, EventDataFrame> readMore(Query query, T storage) throws StatdException {
        throw new UnsupportedOperationException();
    }
}
