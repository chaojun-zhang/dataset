package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public interface Aggregator<T extends Number> {

    void merge(RowProxy row);

    T finish();
}
