package io.github.dflib.storage.aggregator;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.DataFrameBuilder;
import com.nhl.dflib.DataFrameByRowBuilder;
import com.nhl.dflib.DoubleSeries;
import com.nhl.dflib.IntSeries;
import com.nhl.dflib.LongSeries;
import com.nhl.dflib.Series;
import com.nhl.dflib.row.RowProxy;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.dataframe.Field;
import io.github.dflib.dataframe.FieldType;
import io.github.dflib.dataframe.Row;
import io.github.dflib.dataframe.Schema;
import io.github.dflib.dataframe.aggregator.Aggregator;
import io.github.dflib.dataframe.aggregator.CountAggregator;
import io.github.dflib.dataframe.aggregator.DoubleMaxAggregator;
import io.github.dflib.dataframe.aggregator.DoubleMinAggregator;
import io.github.dflib.dataframe.aggregator.DoubleSumAggregator;
import io.github.dflib.dataframe.aggregator.IntMaxAggregator;
import io.github.dflib.dataframe.aggregator.IntMinAggregator;
import io.github.dflib.dataframe.aggregator.IntSumAggregator;
import io.github.dflib.dataframe.aggregator.LongMaxAggregator;
import io.github.dflib.dataframe.aggregator.LongMinAggregator;
import io.github.dflib.dataframe.aggregator.LongSumAggregator;
import io.github.dflib.dataframe.aggregator.AvgAggregator;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.Interval;
import io.github.dflib.query.Metric;
import io.github.dflib.storage.config.AggregatorStorage;
import io.github.dflib.dataframe.aggregator.PercentileAggregator;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class HashAggregator {
    private final Map<Row, Accumulators> aggregateBuffer = new ConcurrentHashMap<>();
    private final Row DUMMY = Row.create();
    private final String eventField;
    private final AggregatorStorage.Aggregator aggregator;
    private final Interval interval;
    private final Schema schema;


    public HashAggregator(String eventField,
                          AggregatorStorage.Aggregator aggregator,
                          Interval interval,Schema sourceSchema) {
        this.eventField = Objects.requireNonNull(eventField);
        this.aggregator = Objects.requireNonNull(aggregator);
        this.interval = interval;
        this.schema = createTargetSchema(sourceSchema);
    }


    private Aggregator<?> createAggregator(Metric metric, DataFrame input) {
        String name = metric.getName();
        Series<?> column = input.getColumn(name);
        switch (metric.getAggregate()) {
            case MIN:
                if (column instanceof IntSeries) {
                    return new IntMinAggregator(name);
                } else if (column instanceof LongSeries) {
                    return new LongMinAggregator(name);
                } else if (column instanceof DoubleSeries) {
                    return new DoubleMinAggregator(name);
                }
                break;
            case MAX:
                if (column instanceof IntSeries) {
                    return new IntMaxAggregator(name);
                } else if (column instanceof LongSeries) {
                    return new LongMaxAggregator(name);
                } else if (column instanceof DoubleSeries) {
                    return new DoubleMaxAggregator(name);
                }
                break;
            case COUNT:
                return new CountAggregator(name);
            case AVG:
                return new AvgAggregator(name);
            case SUM:
                if (column instanceof IntSeries) {
                    return new IntSumAggregator(name);
                } else if (column instanceof LongSeries) {
                    return new LongSumAggregator(name);
                } else if (column instanceof DoubleSeries) {
                    return new DoubleSumAggregator(name);
                }
                break;
            case INTERVAL_95:
            case INTERVAL_MAX:
            case DAY_MAX_AVG:
            case DAY_95_AVG:
            case DAY_95_MAX:
                return new PercentileAggregator(name, eventField, interval, metric.getAggregate());
        }
        throw new IllegalArgumentException("unexpected aggregate type:" + metric.getAggregate());
    }


    public void merge(EventDataFrame dataFrame) {
        dataFrame.getTable().forEach(row -> {
            final Row groupingRow = createGroupingRow(row);
            aggregateBuffer.compute(groupingRow, (k, it) -> {
                if (it == null) {
                    List<Aggregator<?>> aggregators = aggregator.getMetrics().stream()
                            .map(metric -> this.createAggregator(metric, dataFrame.getTable())).collect(Collectors.toList());
                    it = new Accumulators(aggregators);
                }
                it.merge(row);
                return it;
            });
        });
    }


    private Row createGroupingRow(RowProxy source) {
        if ((aggregator.getDimensions() == null || aggregator.getDimensions().length == 0)
                && Granularity.isAllGranularity(aggregator.getGranularity())) {
            return DUMMY;
        } else {
            List<Object> values = new LinkedList<>();
            if (!Granularity.isAllGranularity(aggregator.getGranularity())) {
                Object timestamp = source.get(eventField);
                LocalDateTime dateTime = aggregator.getGranularity().getDateTime(new Timestamp((long) timestamp).toLocalDateTime());
                values.add(Timestamp.valueOf(dateTime).getTime());
            }
            if (aggregator.getDimensions() != null) {
                for (String dimension : aggregator.getDimensions()) {
                    values.add(source.get(dimension));
                }
            }

            return Row.create(values.toArray());
        }

    }

    private static class Accumulators {
        private final List<Aggregator<?>> accumulators;

        private Accumulators(List<Aggregator<?>> accumulators) {
            this.accumulators = accumulators;
        }

        public void merge(RowProxy row) {
            accumulators.forEach(aggregator -> aggregator.merge(row));
        }

        public Row result() {
            Object[] row = accumulators.stream().map(Aggregator::finish).toArray();
            return Row.create(row);
        }
    }

    public DataFrame toDataFrame() {
        DataFrameByRowBuilder dataFrameByRowBuilder = DataFrameBuilder.builder(schema.allColumnLabels()).byRow(schema.accumulators());
        aggregateBuffer.forEach((dimension, metrics) -> {
            if (dimension.isEmpty()) {
                Object[] row = metrics.accumulators.stream().map(Aggregator::finish).toArray();
                dataFrameByRowBuilder.addRow(row);
            } else {
                Object[] row = ArrayUtils.addAll(dimension.getData(), metrics.accumulators.stream().map(Aggregator::finish).toArray());
                dataFrameByRowBuilder.addRow(row);
            }
        });
        return dataFrameByRowBuilder.create();
    }

    private Schema createTargetSchema(Schema sourceSchema) {
        List<Field> fields = new ArrayList<>();
        if (!Granularity.isAllGranularity(aggregator.getGranularity())) {
            fields.add(new Field(eventField, FieldType.INSTANT));
        }
        if (aggregator.getDimensions() != null) {
            Arrays.stream(aggregator.getDimensions()).forEach(it-> {
                fields.add(sourceSchema.getCheckedField(it));
            });
        }
        if (aggregator.getMetrics() != null) {
           aggregator.getMetrics().forEach(it-> {
               Field metricField = sourceSchema.getCheckedField(it.getName());
               Field targetField = new Field(it.getAs(), metricField.getType());
               fields.add(targetField);
            });
        }
        return new Schema(fields);
    }

}
