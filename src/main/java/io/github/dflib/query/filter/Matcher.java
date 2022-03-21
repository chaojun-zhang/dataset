package io.github.dflib.query.filter;

import io.github.dflib.query.Query;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
public final class Matcher implements Filter{

    private String field;

    private String pattern;

    public Matcher(String dimension, String pattern) {
        this.field = Objects.requireNonNull(dimension);
        this.pattern = Objects.requireNonNull(pattern);
    }


    @Override
    public void validate(Query search) {
        Objects.requireNonNull(field);
        Objects.requireNonNull(pattern);

    }


}
