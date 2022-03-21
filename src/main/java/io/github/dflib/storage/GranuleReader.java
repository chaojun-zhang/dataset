package io.github.dflib.storage;

import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.Query;
import io.github.dflib.storage.config.GranuleStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GranuleReader implements StorageReader<GranuleStorage> {

    private final DataFrameReader datasetReader;

    @Autowired
    public GranuleReader(DataFrameReader datasetReader) {
        this.datasetReader = datasetReader;
    }

    @Override
    public EventDataFrame read(Query query, GranuleStorage storage) throws StatdException {
        switch (query.getGranularity()) {
            case GMin:
                return datasetReader.load(query, storage.getOneMinute());
            case G5min:
                return datasetReader.load(query, storage.getFiveMinute());
            case GHour:
                return datasetReader.load(query, storage.getHour());
            case GDay:
                return datasetReader.load(query, storage.getDay());
            case GMonth:
                return datasetReader.load(query, storage.getMonth());
            default:
                throw new IllegalArgumentException(String.format("granule '%s' is not supported for granule reader", query.getGranularity()));
        }
    }


}
