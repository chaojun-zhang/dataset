package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class DoubleMaxAggregator implements Aggregator<Double>{

    private final String name;

    private double resut;

    public DoubleMaxAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        resut += Math.max(resut, (Double) row.get(name));

    }

    @Override
    public Double finish() {
        return resut;
    }

}
