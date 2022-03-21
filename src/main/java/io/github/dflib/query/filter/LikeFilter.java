package io.github.dflib.query.filter;

public interface LikeFilter extends Filter {

    String getField();
    String getValue();
}
