package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class LongMinAggregator implements Aggregator<Long>{

    private final String name;

    private long resut;

    public LongMinAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += Math.min(resut, (Long) row.get(name));

    }

    @Override
    public Long finish() {
        return resut;
    }

}
