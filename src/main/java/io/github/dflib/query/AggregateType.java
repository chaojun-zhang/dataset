package io.github.dflib.query;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Optional;

public enum AggregateType {
    SUM("sum"),
    AVG("avg"),
    MAX("max"),
    MIN("min"),
    COUNT("count"),
    INTERVAL_95("interval_95"),//区间95
    INTERVAL_MAX("interval_max"),//区间峰值
    DAY_95_AVG("day_95_avg"),//日95平均
    DAY_MAX_AVG("day_max_avg"),//日峰值平均
    DAY_95_MAX("day_95_max");//日95峰值

    String name;

    AggregateType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public boolean isNormal() {
        return this == SUM || this == AVG || this == MAX || this == MIN || this == COUNT;
    }

    public static Optional<AggregateType> from(String type) {
        return Arrays.stream(AggregateType.values()).filter(it -> it.getName().equalsIgnoreCase(type)).findFirst();
    }

    public boolean isInterval() {
        return this == INTERVAL_95 || this == INTERVAL_MAX;
    }
}
