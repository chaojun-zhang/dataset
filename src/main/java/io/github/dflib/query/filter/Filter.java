package io.github.dflib.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.dflib.query.Query;
import io.github.dflib.core.parser.filter.FilterCompiler;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EqualTo.class, name = "eq"),
        @JsonSubTypes.Type(value = NotEqualTo.class, name = "ne"),
        @JsonSubTypes.Type(value = LessThan.class, name = "lt"),
        @JsonSubTypes.Type(value = LessAndEqualTo.class, name = "lte"),
        @JsonSubTypes.Type(value = GreaterThan.class, name = "gt"),
        @JsonSubTypes.Type(value = GreaterAndEualTo.class, name = "gte"),
        @JsonSubTypes.Type(value = StartWith.class, name = "startWith"),
        @JsonSubTypes.Type(value = EndWith.class, name = "endWith"),
        @JsonSubTypes.Type(value = Matcher.class, name = "match"),
        @JsonSubTypes.Type(value = Contain.class, name = "contain"),
        @JsonSubTypes.Type(value = And.class, name = "and"),
        @JsonSubTypes.Type(value = Or.class, name = "or"),
        @JsonSubTypes.Type(value = Not.class, name = "not"),
        @JsonSubTypes.Type(value = StringIn.class, name = "in")}
)
public interface Filter {

    default Filter and(Filter filter) {
        return new And(this, filter);
    }

    default Filter or(Filter filter) {
        return new Or(this, filter);
    }

    default Filter not() {
        return new Not(this);
    }

    void validate(Query search);

    static Filter compile(String expr) {
        return FilterCompiler.compile(expr).getOrNull();
    }

    static Filter startWith(String field, String value) {
        return new StartWith(field, value);
    }
    static Filter endWith(String field, String value) {
        return new EndWith(field, value);
    }
    static Filter contains(String field, String value) {
        return new Contain(field, value);
    }
    static Filter eq(String field, Object value) {
        return new EqualTo(field, value);
    }
    static Filter ne(String field, Object value) {
        return new NotEqualTo(field, value);
    }
    static Filter gt(String field, Number value) {
        return new GreaterThan(field, value);
    }
    static Filter lt(String field, Number value) {
        return new LessThan(field, value);
    }
    static Filter gte(String field, Number value) {
        return new GreaterAndEualTo(field, value);
    }
    static Filter lte(String field, Number value) {
        return new LessAndEqualTo(field, value);
    }
    static Filter match(String field, String value) {
        return new Matcher(field, value);
    }
    static Filter inStringCollections(String field, List<String> values) {
        return new StringIn(field, values,false);
    }
    static Filter notInStringCollections(String field, List<String> values) {
        return new StringIn(field, values,true);
    }
    static Filter inNumberCollections(String field, List<Number> values) {
        return new NumberIn(field, values,false);
    }
    static Filter notInNumberCollections(String field, List<Number> values) {
        return new NumberIn(field, values,true);
    }
}
