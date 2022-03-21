package io.github.dflib.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.dflib.exception.StatdException;
import io.github.dflib.storage.GranuleReader;
import io.github.dflib.exception.StorageConfigError;
import io.vavr.collection.Iterator;
import lombok.Data;

import java.util.Objects;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GranuleStorage implements SingleStorage {

    @JsonProperty("1min")
    private SingleStorage oneMinute;

    @JsonProperty("5min")
    private SingleStorage fiveMinute;
    @JsonProperty("hour")
    private SingleStorage hour;
    @JsonProperty("day")
    private SingleStorage day;
    @JsonProperty("month")
    private SingleStorage month;

    @Override
    @JsonIgnore
    public Class<GranuleReader> getReaderClass() {
        return GranuleReader.class;
    }


    @Override
    public void validate() {
        boolean atLeastOneExists = Iterator.of(oneMinute, fiveMinute, hour, day, month).exists(Objects::nonNull);
        if (!atLeastOneExists) {
            throw new StatdException(StorageConfigError.GranuleStorageConfigError, "granule storage is null");
        }
        Iterator.of(oneMinute, fiveMinute, hour, day, month).filter(Objects::nonNull).forEach(Storage::validate);

    }


}
