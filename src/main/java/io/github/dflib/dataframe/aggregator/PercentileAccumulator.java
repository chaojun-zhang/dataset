package io.github.dflib.dataframe.aggregator;

import io.github.dflib.dflib.exp.TimestampPair;
import io.github.dflib.query.AggregateType;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.Interval;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.ToDoubleFunction;

public class PercentileAccumulator {
    private static final Double ZERO_DOUBLE = 0.;
    private int bufferSize;
    private Map<LocalDate, PriorityQueue<Double>> aggBuffer;
    private static final LocalDate DUMMY_DAY = LocalDate.MIN;
    private boolean isInterval;
    private final AggregateType aggregateType;
    private final Interval interval;

    public PercentileAccumulator(AggregateType aggregateType, Interval interval) {
        this.aggBuffer = new HashMap<>();
        this.aggregateType = aggregateType;
        this.interval = interval;
        this.bufferSize = getBufferSizeForInterval();
        this.isInterval = aggregateType == AggregateType.INTERVAL_95 || aggregateType == AggregateType.INTERVAL_MAX;
    }

    public PercentileAccumulator(AggregateType aggregateType) {
        this(aggregateType, null);
    }

    public int getBufferSizeForInterval() {
        switch (aggregateType) {
            case DAY_95_AVG:
            case DAY_95_MAX:
                return 15;
            case DAY_MAX_AVG:
            case INTERVAL_MAX:
                return 1;
            case INTERVAL_95: {
                return Double.valueOf(Math.ceil(interval.slots(Granularity.G5min) * 0.05)).intValue();
            }
            default:
                throw new UnsupportedOperationException("percentile accumulator not support for " + aggregateType);
        }
    }

    public void collect(TimestampPair element) {
        LocalDate day = element.day();
        if (isInterval) {
            day = DUMMY_DAY;
        }
        aggBuffer.compute(day, (k, v) -> {
            if (v == null) {
                v = new PriorityQueue<>();
            }
            double value = element.getValue().doubleValue();
            v.add(value);
            if (v.size() > bufferSize) {
                v.poll();
            }
            return v;
        });
    }

    public Double finish() {
        if (aggBuffer.isEmpty()) {
            return ZERO_DOUBLE;
        } else {
            //如果队列未达到预计大小，表示有含义的数值不足，返回0（与事先补0效果相同）
            ToDoubleFunction<PriorityQueue<Double>> getValueFromQueue = priorityQueue -> {
                if (priorityQueue.size() >= bufferSize) {
                    return priorityQueue.peek();
                } else {
                    return ZERO_DOUBLE;
                }
            };

            if (aggregateType == AggregateType.DAY_95_MAX) {
                return aggBuffer.values().stream().mapToDouble(getValueFromQueue).max().orElse(ZERO_DOUBLE);
            } else {
                return aggBuffer.values().stream().mapToDouble(getValueFromQueue).average().orElse(ZERO_DOUBLE);
            }
        }
    }

    //因为不同的group使用相同的aggregator，所以每个group计算前都需要清空中间状态
    public Double agg(Iterable<TimestampPair> elements) {
        aggBuffer.clear();
        elements.forEach(this::collect);
        return finish();
    }

    public Double agg(TimestampPair[] elements) {
        aggBuffer.clear();
        for (TimestampPair element : elements) {
            this.collect(element);
        }
        return finish();
    }
}