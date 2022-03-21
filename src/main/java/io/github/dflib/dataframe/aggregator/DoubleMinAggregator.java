package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class DoubleMinAggregator implements Aggregator<Double>{

    private final String name;

    private double resut;

    public DoubleMinAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += Math.min(resut, (Double) row.get(name));

    }

    @Override
    public Double finish() {
        return resut;
    }

}
