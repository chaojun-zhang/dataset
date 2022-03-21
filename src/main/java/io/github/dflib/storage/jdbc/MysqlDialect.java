package io.github.dflib.storage.jdbc;

import io.github.dflib.query.Granularity;
import io.github.dflib.storage.config.JdbcStorage;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class MysqlDialect extends SqlDialect {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private final JdbcStorage storage;

    public MysqlDialect(JdbcStorage storage) {
        this.storage = storage;
    }

    @Override
    public String timePeriod(Granularity granularity) {
        if (Granularity.GAll == granularity) {
            return storage.getEventTimeField();
        }
        String timestampField = storage.getEventTimeField();
        switch (granularity) {
            case G5min:
                return String.format("DATE_FORMAT(%s, '%s') + INTERVAL (MINUTE(%s) - MINUTE(%s) MOD 5) MINUTE", timestampField, "%Y-%m-%d %H:00", timestampField, timestampField);
            case GHour:
                return String.format("DATE_FORMAT(%s, '%s')", timestampField, "%Y-%m-%d %H:00:00");
            case GDay:
                return String.format("DATE_FORMAT(%s, '%s')", timestampField, "%Y-%m-%d 00:00:00");
            case GMonth:
                return String.format("DATE_FORMAT(%s, '%s')", timestampField, "%Y-%m-01 00:00:00");
            default:
                throw new UnsupportedOperationException(String.format("date truncate function is not supported on granularity %s", granularity));
        }
    }

    @Override
    public String timeRange(LocalDateTime start, LocalDateTime end) {
        String startTime = formatter.format(start);
        String endTime = formatter.format(end);
        return String.format("%s >= '%s' and %s < '%s'", storage.getEventTimeField(), startTime, storage.getEventTimeField(), endTime);
    }


}
