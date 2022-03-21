package io.github.dflib.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.dflib.core.parser.filter.FilterCompiler;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.filter.Filter;
import io.github.dflib.exception.StorageRequestError;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Query {

    //查询的起止时间
    private Interval interval;
    //查询粒度
    private String granularity;
    //查询条件对象
    private Filter filter;
    //查询条件表达式
    private String filterExpr;
    //维度字段
    private String[] dimensions = new String[0];
    //指标字段
    private List<Metric> metrics = new ArrayList<>();
    //是否时序查询
    private boolean timeseries;
    //limit
    private int limit;


    public void setFilterExpr(String value) {
        this.filterExpr = value;
        this.filter = FilterCompiler.compile(value).getOrNull();
    }

    public Query withInterval(Interval interval) {
        Query query = new Query(this);
        query.setInterval(interval);
        return query;
    }

    public Query(Query query) {
        this.interval = query.interval;
        this.granularity = query.granularity;
        this.filterExpr = query.filterExpr;
        this.filter = query.filter;
        this.metrics = new ArrayList<>();
        this.metrics.addAll(query.metrics);
        this.dimensions = Arrays.copyOf(query.dimensions, query.dimensions.length);
        this.limit = query.limit;
        this.timeseries = query.timeseries;
    }

    public Filter getFilter() {
        if (filter != null) {
            return filter;
        }
        if (filterExpr != null) {
            return FilterCompiler.compile(filterExpr).getOrNull();
        }
        return null;
    }


    public void validate() throws StatdException {
        if (this.interval == null) {
            throw new StatdException(StorageRequestError.InvalidInterval, "Invalid interval");
        } else {
            interval.validate();
        }

        if (isTimeseries() && Granularity.isAllGranularity(getGranularity())) {
            throw new StatdException(StorageRequestError.InvalidGranule, "Invalid query granularity");
        }

        if (this.dimensions != null && this.dimensions.length > 1 && isTimeseries()) {
            throw new StatdException(StorageRequestError.InvalidDimensions, "Invalid Request, dimension's field must less than 2 for time series query");
        }

        if (filter != null) {
            filter.validate(this);
        }


    }

    public Granularity getGranularity() {
        return Granularity.from(granularity);
    }

    public void setFrom(LocalDateTime time) {
        if (interval == null) {
            interval = new Interval();
        }
        interval.setFrom(time);
    }

    public void setTo(LocalDateTime time) {
        if (interval == null) {
            interval = new Interval();
        }
        interval.setTo(time);
    }

    public LocalDateTime getFrom() {
        if (interval != null) {
            return interval.getFrom();
        }
        return null;
    }

    public LocalDateTime getTo() {
        if (interval != null) {
            return interval.getTo();
        }
        return null;
    }
}
