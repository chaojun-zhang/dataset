package io.github.dflib.dflib.exp;

import com.nhl.dflib.Exp;
import com.nhl.dflib.Series;
import com.nhl.dflib.exp.map.MapExpScalar2;

import java.util.function.BiFunction;


public class InstantExpScalar2<L, R> extends MapExpScalar2<L, R, Long> implements InstantExp {

    public static <L, R> InstantExpScalar2<L, R> mapVal(String opName, Exp<L> left, R right, BiFunction<L, R, Long> op) {
        return new InstantExpScalar2<>(opName, left, right, valToSeries(op));
    }

    public InstantExpScalar2(String opName, Exp<L> left, R right, BiFunction<Series<L>, R, Series<Long>> op) {
        super(opName, Long.class, left, right, op);
    }
}
