package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class LongSumAggregator implements Aggregator<Long>{

    private final String name;

    private long resut;

    public LongSumAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += (Long) row.get(name);
    }

    @Override
    public Long finish() {
        return resut;
    }

}
