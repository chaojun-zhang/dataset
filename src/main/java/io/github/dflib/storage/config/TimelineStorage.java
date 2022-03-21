package io.github.dflib.storage.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.dflib.exception.StatdException;
import io.github.dflib.storage.TimelineReader;
import io.github.dflib.exception.StorageConfigError;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TimelineStorage implements SingleStorage {

    @JsonProperty("old")
    private SingleStorage batch;
    @JsonProperty("new")
    private SingleStorage stream;

    @Override
    public Class<TimelineReader> getReaderClass() {
        return TimelineReader.class;
    }


    @Override
    public void validate() {
        if (batch == null && stream == null) {
            throw new StatdException(StorageConfigError.TimelineStorageConfigError, "at least one of batch or stream configure is required");
        }

        if (batch != null) {
            batch.validate();
        }
        if (stream != null) {
            stream.validate();
        }
    }


}
