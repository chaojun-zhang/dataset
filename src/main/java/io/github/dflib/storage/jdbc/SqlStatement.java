package io.github.dflib.storage.jdbc;

import io.github.dflib.query.Granularity;
import io.github.dflib.query.Metric;
import io.github.dflib.query.Query;
import io.github.dflib.storage.config.JdbcStorage;
import io.vavr.control.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SqlStatement {


    private final SqlDialect sqlDialect;
    private final Query query;
    private final JdbcStorage storage;
    private final List<Object> args = new ArrayList<>();
    private final SqlFilter sqlFilter;

    public SqlStatement(Query query, JdbcStorage storage) {
        this.sqlDialect = getSqlDialect(storage);
        this.query = query;
        this.storage = storage;
        this. sqlFilter = new SqlFilter(args, sqlDialect);
    }

    private SqlDialect getSqlDialect(JdbcStorage storage) {
        switch (storage.getStorageType()) {
            case Druid:
                return new DruidDialect(storage);
            case MySql:
                return new MysqlDialect(storage);
            default:
                throw new IllegalArgumentException();
        }
    }


    private List<Projection> parseDimensionProjections(Query query) {
        if (query.getDimensions() != null) {
            return io.vavr.collection.List.of(query.getDimensions()).map(it -> Projection.dimension(it, it)).toJavaList();
        }else {
            return Collections.emptyList();
        }
    }

    private Option<Projection> parseEventProjection(Granularity granularity) {
        if (Granularity.isAllGranularity(granularity)) {
            return Option.none();
        }

        return Option.of(Projection.event(sqlDialect.timePeriod(granularity), storage));
    }


    public String buildSqlFromTemplate() {
        String filterSql = sqlFilter.toSql(query);

        Map<String, Object> params = new HashMap<>();
        params.put("filter", filterSql);
        params.put("period",sqlDialect.timePeriod(query.getGranularity()));
        return TemplateUtil.generateTemplate(storage.getSql(), params);
    }

    public String toSql() {
        //保证toSql可以幂等调用
        args.clear();
        if (storage.isSqlNotEmpty()) {
            return buildSqlFromTemplate();
        }else {
            return buildSqlFromQuery();
        }


    }

    private String buildSqlFromQuery() {
        final Option<Projection> eventProjection = this.parseEventProjection(query.getGranularity());
        final List<Projection> dimensionProjections = this.parseDimensionProjections(query);
        final List<Projection> metricProjections = this.parseMetricProjections(query);

        final String projectionClause = this.buildProjectionClause(eventProjection, dimensionProjections, metricProjections);

        final String whereClause = sqlFilter.toSql(query);
        final Option<String> groupByClause = this.buildGroupByClause(eventProjection, dimensionProjections);
        final Option<String> orderByClause = this.buildOrderClause(eventProjection);

        final List<String> singleSqlStatement = new ArrayList<>();
        singleSqlStatement.add(SqlKeyWord.KEYWORD_SELECT);
        singleSqlStatement.add(projectionClause);
        singleSqlStatement.add(SqlKeyWord.KEYWORD_FROM);
        singleSqlStatement.add(storage.getTable());
        singleSqlStatement.add(SqlKeyWord.KEYWORD_WHERE);
        singleSqlStatement.add(whereClause);
        if (groupByClause.isDefined()) {
            singleSqlStatement.add(SqlKeyWord.KEYWORD_GROUP_BY);
            singleSqlStatement.add(groupByClause.get());
        }
        if (orderByClause.isDefined()) {
            singleSqlStatement.add(SqlKeyWord.KEYWORD_ORDER_BY);
            singleSqlStatement.add(orderByClause.get());
        }

        if (query.getLimit() > 0) {
            singleSqlStatement.add(SqlKeyWord.STATEMENT_DELIMITER);
            singleSqlStatement.add("" + query.getLimit());
        }
        return String.join(SqlKeyWord.STATEMENT_DELIMITER, singleSqlStatement);
    }


    private Option<String> buildOrderClause(Option<Projection> eventProjection ) {
        return eventProjection.map(it -> it.expression);
    }



    private Option<String> buildGroupByClause(Option<Projection> eventProjection, List<Projection> dimensionProjections) {
        //满足聚合查询
        if (isAggregateQuery()) {
            final List<String> groupBy = new ArrayList<>();
            eventProjection.forEach(it -> groupBy.add(it.expression));
            dimensionProjections.forEach(it -> groupBy.add(it.expression));
            if (!groupBy.isEmpty()) {
                return Option.of(String.join(SqlKeyWord.FIELD_DELIMITER, groupBy));
            }
        }
        return Option.none();
    }

    private String buildProjectionClause(Option<Projection> eventProjection,
                                         List<Projection> dimensionProjections,
                                         List<Projection> metricProjections) {
        final List<String> projections = new ArrayList<>();
        if (eventProjection.isDefined()) {
            projections.add(eventProjection.get().toSql());
        }
        for (Projection dimensionProjection : dimensionProjections) {
            projections.add(dimensionProjection.toSql());
        }
        for (Projection metricProjection : metricProjections) {
            projections.add(metricProjection.toSql());
        }
        return String.join(SqlKeyWord.FIELD_DELIMITER, projections);
    }


    private List<Projection> parseMetricProjections(Query query) {
        final Function<Metric, Projection> metricToProjection = metric -> {
            if (isAggregateQuery()) {
                String aggregateExpression = sqlDialect.aggregateFunction(metric);
                String expression = String.format("%s", aggregateExpression);
                return Projection.metric(expression, metric.getAs());
            } else { //如果不是聚合查询，则不用使用聚合函数
                return Projection.metric(metric.getName(), metric.getAs());
            }
        };
        return query.getMetrics().stream().map(metricToProjection).collect(Collectors.toList());
    }


    private static final class Projection {
        private final String expression;
        private final String fieldName;

        private Projection(String expression, String fieldName) {
            this.expression = expression;
            this.fieldName = fieldName;
        }

        public static Projection event(String name, JdbcStorage storage) {
            return new Projection(name, storage.getEventTimeField());
        }

        public static Projection dimension(String name, String fieldName) {
            return new Projection(name, fieldName);
        }

        public static Projection metric(String name, String fieldName) {
            return new Projection(name, fieldName);
        }

        public String toSql() {
            return String.format("%s as %s", expression, fieldName);
        }


    }

    public Object[] getArguments() {
        return args.toArray();
    }

    public static SqlStatement create(Query query, JdbcStorage storage) {
        return new SqlStatement(query, storage);
    }

    private boolean isAggregateQuery() {
        //维度为空，并且查询和底层粒度相同
        if ((query.getDimensions() == null || query.getDimensions().length == 0)
                && storage.getGranularity() == query.getGranularity()) {
            return true;
        }

        //如果是所有粒度
        //查询粒度大于存储粒度
        //查询维度不为空
        return Granularity.isAllGranularity(query.getGranularity())
                || query.getGranularity().isGreaterThan(storage.getGranularity())
                || (query.getDimensions() != null && query.getDimensions().length > 0);


    }
}
