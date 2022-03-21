package io.github.dflib.dataframe.aggregator;

import com.nhl.dflib.row.RowProxy;

public class AvgAggregator implements Aggregator<Double>{

    private final String name;

    private long record;
    private double sum;

    public AvgAggregator(String name) {
        this.name = name;
    }

    @Override
    public void merge(RowProxy row) {
        Double value = (Double) row.get(name);
        if (value != null) {
            sum += value;
        }
        record++;

    }

    @Override
    public Double finish() {
        if (record == 0) {
            return 0.;
        }

        return sum / record;
    }

}
