package io.github.dflib.storage.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.Metric;
import io.github.dflib.storage.StorageReader;
import io.github.dflib.storage.aggregator.AggregatorReader;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 主要针对95计算,采用流式计算
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AggregatorStorage implements MultipleStorage {

    private SourceStorage source;

    private Map<String, Aggregator> aggregators;


    @Data
    public static class Aggregator {
        private String[] dimensions;
        private List<Metric> metrics;
        private Granularity granularity;
        private boolean timeseries;
    }


    @Override
    public Class<? extends StorageReader> getReaderClass() {
        return AggregatorReader.class;
    }

    @Override
    public void validate() {
        Objects.requireNonNull(source, " source is required for AggregatorStorage");
        Objects.requireNonNull(aggregators, " aggregators is required for AggregatorStorage");
    }

}
