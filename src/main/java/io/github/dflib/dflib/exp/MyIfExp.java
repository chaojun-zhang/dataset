package io.github.dflib.dflib.exp;

import com.nhl.dflib.Condition;
import com.nhl.dflib.Exp;
import com.nhl.dflib.IntSeries;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.exp.flow.IfExp;
import io.github.dflib.dflib.AccumulatorFactory;

public class MyIfExp extends IfExp {

    public MyIfExp(Condition condition, Exp ifTrueExp, Exp ifFalseExp) {
        super(condition, ifTrueExp, ifFalseExp);
    }

    //重载使用accumulator可以获取精确的字段类型
    protected Series evalMerge(Series dataIfTrue, Series dataIfFalse, IntSeries indexTrue, IntSeries indexFalse) {
        int st = dataIfTrue.size();
        int sf = dataIfFalse.size();
        Accumulator accumulator = AccumulatorFactory.get(getType(), st + sf);
        for (int i = 0; i < st; i++) {
            accumulator.set(indexTrue.getInt(i), dataIfTrue.get(i));
        }
        for (int i = 0; i < sf; i++) {
            accumulator.set(indexFalse.getInt(i), dataIfFalse.get(i));
        }
        return  accumulator.toSeries();
    }
}
