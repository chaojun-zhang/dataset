package io.github.dflib.query.filter;


public interface FieldFilter extends Filter {

    Object getValue();

    String getField();

}
