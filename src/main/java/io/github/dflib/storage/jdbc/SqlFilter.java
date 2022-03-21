package io.github.dflib.storage.jdbc;

import io.github.dflib.query.Query;
import io.github.dflib.query.filter.EndWith;
import io.github.dflib.query.filter.FieldFilter;
import io.github.dflib.query.filter.Filter;
import io.github.dflib.query.filter.StringIn;
import io.github.dflib.query.filter.LikeFilter;
import io.github.dflib.query.filter.LogicalFilter;
import io.github.dflib.query.filter.Matcher;
import io.github.dflib.query.filter.Not;
import io.github.dflib.query.filter.StartWith;
import lombok.Getter;

import java.util.List;

public class SqlFilter {

    @Getter
    private final List<Object> placeHolders;

    private final SqlDialect sqlDialect;

    public SqlFilter(List<Object> placeHolders, SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
        this.placeHolders = placeHolders;
    }

    public String toSql(Query query) {
        if (query.getFilter() != null) {
            final String timeRange = sqlDialect.timeRange(query.getFrom(), query.getTo());
            final String filter = parseFilter(query.getFilter());
            return String.join(SqlKeyWord.KEYWORD_AND, timeRange, filter);
        } else {
            return sqlDialect.timeRange(query.getFrom(), query.getTo());
        }
    }


    private String parseFilter(Filter filter) {
        String filterOp = sqlDialect.filterOp(filter);
        if (filter instanceof FieldFilter) {
            FieldFilter fieldFilter = (FieldFilter) filter;
            Object fieldValue = fieldFilter.getValue();
            this.placeHolders.add(fieldValue);
            return String.format("%s %s ?", fieldFilter.getField(), filterOp);
        } else if (filter instanceof LikeFilter) {
            LikeFilter likeFilter = (LikeFilter) filter;
            if (filter instanceof StartWith) {
                this.placeHolders.add("%" + likeFilter.getValue());
            } else if (filter instanceof EndWith) {
                this.placeHolders.add(likeFilter.getValue() + "%");
            } else {
                this.placeHolders.add("%" + likeFilter.getValue() + "%");
            }
            return String.format("%s %s ?", likeFilter.getField(), filterOp);
        } else if (filter instanceof Matcher) {
            Matcher matcher = (Matcher) filter;
            String matcherPattern = matcher.getPattern();
            this.placeHolders.add(matcherPattern);
            return String.format("%s %s ?", matcher.getField(), filterOp);
        } else if (filter instanceof StringIn) {
            StringIn in = (StringIn) filter;
            this.placeHolders.add(in.getValues());
            return String.format("%s %s (?)", in.getField(), filterOp);
        } else if (filter instanceof Not) {
            Not not = (Not) filter;
            return String.format("%s (%s)", filterOp, parseFilter(not.getChild()));
        } else if (filter instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) filter;
            String left = this.parseFilter(logicalFilter.getLeft());
            String right = this.parseFilter(logicalFilter.getRight());
            return String.format("(%s %s %s)", left, filterOp, right);
        } else {
            throw new UnsupportedOperationException(String.format("filter '%s' is not supported", filter));
        }
    }
}
