package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class CountAggregator implements Aggregator<Long>{

    private final String name;

    private long resut;

    public CountAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut ++;

    }

    @Override
    public Long finish() {
        return resut;
    }

}
