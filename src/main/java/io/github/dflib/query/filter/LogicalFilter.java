package io.github.dflib.query.filter;

public interface LogicalFilter extends Filter {

    Filter getLeft();

    Filter getRight();
}
