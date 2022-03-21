package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class IntSumAggregator implements Aggregator<Integer>{

    private final String name;

    private int resut;

    public IntSumAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += (Integer) row.get(name);
    }

    @Override
    public Integer finish() {
        return resut;
    }

}
