package io.github.dflib.dataframe;


import com.nhl.dflib.DataFrameBuilder;
import com.nhl.dflib.DataFrameByRowBuilder;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.row.RowProxy;
import io.github.dflib.query.Granularity;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class TimeSeries {
    private static final Row EMPTY_ROW = Row.create();

    private final static DateTimeFormatter DF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final EventDataFrame input;
    private final LocalDateTime from;
    private final LocalDateTime to;
    private final Schema tableSchema;

    private final Field eventTimeField;

    public TimeSeries(EventDataFrame input) {
        this.input = Objects.requireNonNull(input);
        this.from = input.getInterval().getFrom();
        this.to = input.getInterval().getTo();
        this.tableSchema = input.getSchema();
        if (!tableSchema.getEventTimeField().isPresent() || Granularity.isAllGranularity(input.getGranularity())) {
            throw new IllegalStateException("time series only support for granularity query");
        }
        this.eventTimeField = tableSchema.getEventTimeField().get();

    }


    /**
     * 构建时序数据，先找出所有缺失维度和缺失的时间进行填充
     *
     * @return timestamp->dimension->metric
     */
    private Map<LocalDateTime, Map<Row, Row>> fillZero() {
        final Map<LocalDateTime, Map<Row, Row>> eventToDimensionMetric = new HashMap<>();
        Set<Row> dimensionRows = new HashSet<>();
        input.getTable().forEach(row -> {
            Timestamp eventTimeValue = new Timestamp((Long) row.get(eventTimeField.getName()));

            LocalDateTime timestamp = eventTimeValue.toLocalDateTime();
            Row dimension = createDimensionRow(row);
            Row metric = createMetricRow(row);
            eventToDimensionMetric.compute(timestamp, (k, v) -> {
                if (v == null) {
                    v = new HashMap<>();
                }
                v.put(dimension, metric);
                return v;
            });
            dimensionRows.add(dimension);
        });

        Granularity granularity = input.getGranularity();
        LocalDateTime start = granularity.getDateTime(from);
        while (start.isBefore(to)) {
            //fill missing time
            Map<Row, Row> dimensionMetric = eventToDimensionMetric.computeIfAbsent(start, k -> createEmptyDimensionMetrics(dimensionRows));
            for (Row dimension : dimensionRows) {
                //fill missing dimension
                dimensionMetric.computeIfAbsent(dimension, k -> createEmptyMetric());
            }
            eventToDimensionMetric.put(start, dimensionMetric);
            start = granularity.nextTime(start);
        }


        return eventToDimensionMetric;
    }

    public EventDataFrame toDF() {
        String[] columnLabels = tableSchema.allColumnLabels();
        Accumulator[] accumulators = tableSchema.accumulators();

        DataFrameByRowBuilder dataFrameByRowBuilder = DataFrameBuilder.builder(columnLabels).byRow(accumulators);
        Map<LocalDateTime, Map<Row, Row>> rows = fillZero();
        for (Map.Entry<LocalDateTime, Map<Row, Row>> entry : rows.entrySet()) {
            for (Map.Entry<Row, Row> r : entry.getValue().entrySet()) {
                Object[] value = new Object[]{entry.getKey().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()};
                value = ArrayUtils.addAll(value, r.getKey().getData());
                value = ArrayUtils.addAll(value, r.getValue().getData());
                dataFrameByRowBuilder.addRow(value);
            }
        }


        return input.withTable(dataFrameByRowBuilder.create());

    }


    private Row createMetricRow(RowProxy row) {
        Object[] rowObject = tableSchema.getMetricFields().stream().map(it -> row.get(it.getName())).toArray();
        return Row.create(rowObject);
    }

    private Row createDimensionRow(RowProxy row) {
        if (CollectionUtils.isEmpty(tableSchema.getDimensionFields())) {
            return EMPTY_ROW;
        }
        Object[] rowObject = tableSchema.getDimensionFields().stream().map(it -> row.get(it.getName())).toArray();
        return Row.create(rowObject);
    }

    private Map<Row, Row> createEmptyDimensionMetrics(Set<Row> dimensions) {
        Map<Row, Row> dimensionMetrics = new HashMap<>();
        dimensions.forEach(dimension -> {
            Row metric = createEmptyMetric();
            dimensionMetrics.put(dimension, metric);
        });
        return dimensionMetrics;
    }

    private Row createEmptyMetric() {
        Object[] objects = tableSchema.getMetricFields().stream().map(it -> {
            if (FieldType.INT == it.getType()) {
                return 0;
            } else if (FieldType.LONG == it.getType()) {
                return 0L;
            } else if (FieldType.DOUBLE == it.getType()) {
                return 0d;
            } else {
                throw new IllegalStateException("Unexpected value: " + input.getTable().getColumn(it.getName()).getNominalType());
            }
        }).toArray();
        return Row.create(objects);
    }


}
