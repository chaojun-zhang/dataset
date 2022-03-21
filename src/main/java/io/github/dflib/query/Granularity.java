package io.github.dflib.query;

import com.fasterxml.jackson.annotation.JsonValue;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.TimeZone;

public enum Granularity {
    //    GNone(0,"none"),//查明细
    GMin(1, "1min"),
    G5min(2, "5min"),
    GHour(3, "hour"),
    GDay(4, "day"),
    GMonth(5, "month"),
    GAll(Integer.MAX_VALUE, "all");//buckets everything into a single bucket

    public final int order;

    private final String name;

    Granularity(int order, String name) {
        this.order = order;
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public boolean isGreaterThan(Granularity g) {
        return this.order > g.order;
    }

    public static Granularity from(String value) {
        switch (value) {
            case "1min":
                return GMin;
            case "5min":
                return G5min;
            case "hour":
                return GHour;
            case "day":
                return GDay;
            case "month":
                return GMonth;
            default:
                return GAll;
        }
    }

    public LocalDateTime getDateTime(LocalDateTime time) {
        switch (this) {
            case GMin:
                return time.truncatedTo(ChronoUnit.MINUTES);
            case G5min:
                return time.withMinute(time.getMinute() - time.getMinute() % 5).truncatedTo(ChronoUnit.MINUTES);
            case GHour:
                return time.truncatedTo(ChronoUnit.HOURS);
            case GDay:
                return time.truncatedTo(ChronoUnit.DAYS);
            case GMonth:
                return LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0, 0);
            case GAll:
                return time;
            default:
                return time;
        }
    }

    public long getDateTime(long time) {
        LocalDateTime t = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), TimeZone.getDefault().toZoneId());
        LocalDateTime gTime = getDateTime(t);
        return gTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

    }

    public LocalDate getDate(Timestamp timestamp) {
        Objects.requireNonNull(timestamp);
        return timestamp.toLocalDateTime().toLocalDate();
    }


    public LocalDateTime nextTime(LocalDateTime time) {
        switch (this) {
            case GMin:
                return time.plusMinutes(1);
            case G5min:
                return time.plusMinutes(5);
            case GHour:
                return time.plusHours(1);
            case GDay:
                return time.plusDays(1);
            case GMonth:
                return time.plusMonths(1);
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    public static boolean isAllGranularity(Granularity granularity) {
        return null == granularity || GAll == granularity;
    }

}