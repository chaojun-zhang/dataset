package io.github.dflib.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@Data
public final class Metric {

    private String name;
    private AggregateType aggregate;
    private String as;

    public void validate() {
        Objects.requireNonNull(name, "name is null");
        Objects.requireNonNull(aggregate, "aggregate is null");
    }

    public String getAs(){
        if (as == null) {
            return name;
        }
        return as;
    }

    public AggregateType getAggregate() {
        if (aggregate == null) {
            return AggregateType.SUM;
        }else {
            return aggregate;
        }
    }

    public static Metric sum(String name) {
        Metric metric = new Metric();
        metric.setName(name);
        metric.setAggregate(AggregateType.SUM);
        metric.setAs(name);
        return metric;
    }


}
