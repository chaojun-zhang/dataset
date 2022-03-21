package io.github.dflib.storage.jdbc;

import io.github.dflib.query.AggregateType;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.Metric;
import io.github.dflib.storage.config.JdbcStorage;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public final class DruidDialect extends SqlDialect {
    private final JdbcStorage storage;

    public DruidDialect(JdbcStorage storage) {
        this.storage = storage;
    }

    @Override
    public String timePeriod(Granularity granularity) {
        if (Granularity.GAll == granularity) {
            return storage.getEventTimeField();
        }
        String period;
        switch (granularity) {
            case GMin:
                period = "PT1M";
                break;
            case G5min:
                period = "PT5M";
                break;
            case GHour:
                period = "PT1H";
                break;
            case GDay:
                period = "P1D";
                break;
            case GMonth:
                period = "P1M";
                break;
            default:
                throw new UnsupportedOperationException(String.format("date truncate function is not supported on granularity %s", granularity));
        }
        return String.format("TIME_FLOOR(%s, '%s', NULL, '+08:00') + INTERVAL '%s' HOUR", storage.getEventTimeField(), period, storage.getZoneOffsetHour());
    }

    @Override
    public String timeRange(LocalDateTime start, LocalDateTime end) {
        String startTime = String.format("MILLIS_TO_TIMESTAMP(%s)", Timestamp.valueOf(start).getTime());
        String endTime = String.format("MILLIS_TO_TIMESTAMP(%s)", Timestamp.valueOf(end).getTime());
        return String.format("%s >= %s and %s < %s", storage.getEventTimeField(), startTime, storage.getEventTimeField(), endTime);

    }

    @Override
    public String aggregateFunction(Metric metric) {
        if (AggregateType.INTERVAL_95 == metric.getAggregate()) {
            return String.format("APPROX_QUANTILE(%s,0.95)", metric.getName());
        } else {
            return super.aggregateFunction(metric);
        }
    }


}
