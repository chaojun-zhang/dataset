package io.github.dflib.storage.jdbc;

import io.github.dflib.query.Granularity;
import io.github.dflib.query.Metric;
import io.github.dflib.query.filter.And;
import io.github.dflib.query.filter.EqualTo;
import io.github.dflib.query.filter.Filter;
import io.github.dflib.query.filter.GreaterAndEualTo;
import io.github.dflib.query.filter.GreaterThan;
import io.github.dflib.query.filter.LessAndEqualTo;
import io.github.dflib.query.filter.LessThan;
import io.github.dflib.query.filter.LikeFilter;
import io.github.dflib.query.filter.Not;
import io.github.dflib.query.filter.NotEqualTo;
import io.github.dflib.query.filter.Or;
import io.github.dflib.query.filter.StringIn;

import java.time.LocalDateTime;

public abstract class SqlDialect {

    abstract public String timePeriod(Granularity granularity);

    abstract public String timeRange(LocalDateTime start, LocalDateTime end);

    public String aggregateFunction(Metric metric) {
        switch (metric.getAggregate()) {
            case COUNT:
                return String.format("count(%s)", metric.getName());
            case MAX:
                return String.format("max(%s)", metric.getName());
            case MIN:
                return String.format("min(%s) ", metric.getName());
            case SUM:
                return String.format("sum(%s)", metric.getName());
            case AVG:
                return String.format("avg(%s) ", metric.getName());
            default:
                throw new IllegalStateException("Unexpected value: " + metric.getAggregate());
        }
    }

    public String filterOp(Filter operator) {
        if (operator instanceof EqualTo) {
            return SqlKeyWord.KEYWORD_EQ;
        } else if (operator instanceof NotEqualTo) {
            return SqlKeyWord.KEYWORD_NE;
        } else if (operator instanceof And) {
            return SqlKeyWord.KEYWORD_AND;
        } else if (operator instanceof Or) {
            return SqlKeyWord.KEYWORD_OR;
        } else if (operator instanceof StringIn) {
            StringIn stringIn = (StringIn) operator;
            if (stringIn.isNegate()) {
                return SqlKeyWord.KEYWORD_NOT_IN;
            } else {
                return SqlKeyWord.KEYWORD_IN;
            }
        } else if (operator instanceof Not) {
            return SqlKeyWord.KEYWORD_NEGTIVE;
        } else if (operator instanceof GreaterThan) {
            return SqlKeyWord.KEYWORD_GT;
        } else if (operator instanceof GreaterAndEualTo) {
            return SqlKeyWord.KEYWORD_GTE;
        } else if (operator instanceof LessThan) {
            return SqlKeyWord.KEYWORD_LT;
        } else if (operator instanceof LessAndEqualTo) {
            return SqlKeyWord.KEYWORD_LTE;
        } else if (operator instanceof LikeFilter) {
            return SqlKeyWord.KEYWORD_LIKE;
        } else {
            throw new UnsupportedOperationException();

        }
    }
}

