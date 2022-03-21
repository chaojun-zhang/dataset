package io.github.dflib.dataframe;

import com.nhl.dflib.DataFrame;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.Interval;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EventDataFrame {
    @Getter
    @Setter
    private Granularity granularity;

    @Getter
    private DataFrame table;

    @Getter
    @Setter
    public Interval interval;

    @Getter
    private Schema schema;

    public EventDataFrame withTable(DataFrame table) {
        EventDataFrame dataFrame = new EventDataFrame();
        dataFrame.setTable(table);
        dataFrame.setInterval(interval);
        dataFrame.setGranularity(granularity);
        return dataFrame;
    }

    public void setTable(DataFrame table) {
        this.table = table;
        this.schema = Schema.createFromDataFrame(table);
    }

    public List<Map<String,Object>> toRows(){
        List<Map<String, Object>> result = new ArrayList<>();
        table.forEach(rowProxy->{
            Map<String,Object> row = new HashMap<>();
            for (String name : rowProxy.getIndex()) {
                row.put(name, rowProxy.get(name));
            }
            result.add(row);
        });
        return result;
    }


    public EventDataFrame toZeroDf() {
        return new TimeSeries(this).toDF();
    }
}
