package io.github.dflib.dflib.exp;

import com.google.common.collect.Sets;
import com.nhl.dflib.BooleanSeries;
import com.nhl.dflib.Condition;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Exp;
import com.nhl.dflib.IntSeries;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import io.github.dflib.dflib.AccumulatorFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CaseWhenExp<T> implements Exp<T> {

    private final List<Condition> conditions;
    private final List<Exp<T>> expList;
    private final Exp<T> alternative;

    public CaseWhenExp(List<Condition> conditions, List<Exp<T>> expList, Exp<T> alternative) {
        this.conditions = Objects.requireNonNull(conditions);
        this.expList = Objects.requireNonNull(expList);
        this.alternative = Objects.requireNonNull(alternative);
    }


    @Override
    public String toString() {
        return toQL();
    }

    @Override
    public Class<T> getType() {
        return alternative.getType();
    }

    @Override
    public String toQL() {
        List<String> whenQL = IntStream.range(0, conditions.size()).mapToObj(it -> {
            Condition condition = conditions.get(it);
            Exp<T> exp = expList.get(it);
            return " when " + condition.toQL() + " then " + exp.toQL();
        }).collect(Collectors.toList());
        return "case " + whenQL  + " else " + alternative.toQL();
    }

    @Override
    public String toQL(DataFrame df) {
        List<String> whenQL = IntStream.range(0, conditions.size()).mapToObj(it -> {
            Condition condition = conditions.get(it);
            Exp<T> exp = expList.get(it);
            return " when " + condition.toQL(df) + " then " + exp.toQL(df);
        }).collect(Collectors.toList());
        return "case " + whenQL  + " else " + alternative.toQL(df);
    }


    @Override
    public Series<T> eval(DataFrame df) {
        Map<Exp<T>, BooleanSeries> branches = new HashMap<>();
        for (int i=0;i<conditions.size();i++) {
            Condition condition = conditions.get(i);
            Exp<T> exp = expList.get(i);
            branches.put(exp, condition.eval(df));
        }

        return evalBranches(branches);
    }

    @Override
    public Series<T> eval(Series<?> s) {
        Map<Exp<T>, BooleanSeries> branches = new HashMap<>();
        for (int i=0;i<conditions.size();i++) {
            Condition condition = conditions.get(i);
            Exp<T> exp = expList.get(i);
            branches.put(exp, condition.eval(s));
        }
        return evalBranches(branches);
    }

    protected Series<T> evalBranches( Map<Exp<T>, BooleanSeries> branches) {

        Optional<IntSeries> alternativeTrue = branches.values().stream()
                .map(BooleanSeries::indexFalse).map(Sets::newHashSet)
                .reduce((a, b) -> {
                    Sets.SetView<Integer> intersection = Sets.intersection(a, b);
                    return new HashSet<>(intersection);
                }).map(it -> IntSeries.forInts(it.stream().mapToInt(i -> i).toArray()));

        int branchTrueSize = branches.values().stream().mapToInt(it->it.indexTrue().size()).sum();
        int alternativeSize = alternativeTrue.map(Series::size).orElse(0);

        Accumulator accumulator = AccumulatorFactory.get(alternative.getType(),branchTrueSize + alternativeSize);
        branches.forEach((exp,mask)->{
            Series<T> data = exp.eval(mask.indexTrue());
            for (Integer position : mask.indexTrue()) {
                T object = data.get(position);
                accumulator.set(position, object);
            }
        });

        if (alternativeTrue.isPresent()) {
            Series<T> dataTrue = alternative.eval(alternativeTrue.get());
            for (int index : alternativeTrue.get()) {
                accumulator.set(index, dataTrue.get(index));
            }
        }
        return accumulator.toSeries();
    }
}
