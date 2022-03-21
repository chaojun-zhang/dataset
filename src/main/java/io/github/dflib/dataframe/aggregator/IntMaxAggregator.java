package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class IntMaxAggregator implements Aggregator<Integer>{

    private final String name;

    private int resut;

    public IntMaxAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += Math.max(resut, (Integer) row.get(name));

    }

    @Override
    public Integer finish() {
        return resut;
    }

}
