package io.github.dflib.storage.aggregator;

import io.github.dflib.storage.DataFrameReader;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.Interval;
import io.github.dflib.query.Query;
import io.github.dflib.storage.StorageReader;
import io.github.dflib.storage.config.AggregatorStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Lazy
public class AggregatorReader implements StorageReader<AggregatorStorage> {

    private final DataFrameReader datasetReader;


    @Autowired
    public AggregatorReader(DataFrameReader datasetReader) {
        this.datasetReader = datasetReader;
    }

    @Override
    public EventDataFrame read(Query query, AggregatorStorage storage) throws StatdException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, EventDataFrame> readMore(Query query, AggregatorStorage storage) throws StatdException {
        final List<Interval> days = query.getInterval().days();
        final EventDataFrame source = datasetReader.load(query.withInterval(days.get(0)), storage.getSource());
        final Map<String, HashAggregator> hashAggregatorMap = createHashAggregator(query, storage, source);
        hashAggregatorMap.values().forEach(hashAggregator -> {
            hashAggregator.merge(source);
        });

        if (days.size() > 1) {
            for (Interval day : days.subList(1, days.size())) {
                final EventDataFrame dataFrame = datasetReader.load(query.withInterval(day), storage.getSource());
                hashAggregatorMap.values().forEach(hashAggregator -> {
                    hashAggregator.merge(dataFrame);
                });
            }
        }

        final Map<String, EventDataFrame> result = new HashMap<>();
        hashAggregatorMap.forEach((name, hashAggregator) -> {
            AggregatorStorage.Aggregator aggregator = storage.getAggregators().get(name);
            EventDataFrame eventDataFrame = new EventDataFrame();
            eventDataFrame.setInterval(query.getInterval());
            eventDataFrame.setGranularity(aggregator.getGranularity());
            eventDataFrame.setTable(hashAggregator.toDataFrame());
            if (aggregator.isTimeseries()) {
                result.put(name, eventDataFrame.toZeroDf());
            } else {
                result.put(name, eventDataFrame);
            }
        });
        return result;
    }

    private Map<String, HashAggregator> createHashAggregator(Query query, AggregatorStorage storage,
                                                             EventDataFrame source) {
        Map<String, HashAggregator> hashAggregatorMap = new HashMap<>();
        for (Map.Entry<String, AggregatorStorage.Aggregator> nameToAggregator : storage.getAggregators().entrySet()) {
            AggregatorStorage.Aggregator aggregator = nameToAggregator.getValue();
            final HashAggregator hashAggregator = new HashAggregator(
                    storage.getSource().getEventTimeField(),
                    aggregator,
                    query.getInterval(),
                    source.getSchema()
            );
            hashAggregatorMap.put(nameToAggregator.getKey(), hashAggregator);
        }
        return hashAggregatorMap;
    }

}
