package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;
import io.github.dflib.dflib.exp.TimestampPair;
import io.github.dflib.query.AggregateType;
import io.github.dflib.query.Interval;

import java.util.Objects;

public class PercentileAggregator implements Aggregator<Double> {

    private final PercentileAccumulator percentileAccumulator;

    private final String eventTime;
    private final String name;

    public PercentileAggregator(String name, String eventTime, Interval interval , AggregateType aggregateType) {
        this.name = Objects.requireNonNull(name);
        this.eventTime = Objects.requireNonNull(eventTime);
        this.percentileAccumulator = new PercentileAccumulator(aggregateType, interval);
    }

    @Override
    public void merge(RowProxy row) {
        Long timestamp = (Long) row.get(eventTime);
        Number fieldValue = (Number)row.get(name);
        TimestampPair timestampPair = new TimestampPair();
        timestampPair.setTimestamp(timestamp);
        timestampPair.setValue(fieldValue);
        this.percentileAccumulator.collect(timestampPair);

    }

    @Override
    public Double finish() {
        return percentileAccumulator.finish();
    }

}
