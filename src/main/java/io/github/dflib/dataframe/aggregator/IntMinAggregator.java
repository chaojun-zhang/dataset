package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class IntMinAggregator implements Aggregator<Integer>{

    private final String name;

    private Integer resut;

    public IntMinAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += Math.min(resut, (Integer) row.get(name));

    }

    @Override
    public Integer finish() {
        return resut;
    }

}
