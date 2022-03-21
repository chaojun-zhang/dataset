package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class LongMaxAggregator implements Aggregator<Long>{

    private final String name;

    private long resut;

    public LongMaxAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += Math.max(resut, (Long) row.get(name));

    }

    @Override
    public Long finish() {
        return resut;
    }

}
