package io.github.dflib.dflib.exp;

import com.nhl.dflib.Exp;
import com.nhl.dflib.Series;
import com.nhl.dflib.exp.Exp2;
import com.nhl.dflib.series.ObjectSeries;

public class TimestampPairExp<T extends Number> extends Exp2<Long, T, TimestampPair> {
    public TimestampPairExp(String opName, Exp<Long> left, Exp<T> right) {
        super(opName, TimestampPair.class, left, right);
    }

    @Override
    protected Series<TimestampPair> doEval(Series<Long> left, Series<T> right) {

       return  new ObjectSeries<TimestampPair>(TimestampPair.class){

            @Override
            public int size() {
                return left.size();
            }

            @Override
            public TimestampPair get(int index) {
                TimestampPair res = new TimestampPair();
                res.setTimestamp(left.get(index));
                res.setValue(right.get(index));
                return res;
            }

            @Override
            public void copyTo(Object[] to, int fromOffset, int toOffset, int len) {

            }

            @Override
            public Series<TimestampPair> materialize() {
                return this;
            }

           @Override
           public Series<TimestampPair> fillNulls(TimestampPair value) {
               return null;
           }

           @Override
           public Series<TimestampPair> fillNullsFromSeries(Series<? extends TimestampPair> values) {
               return null;
           }

           @Override
           public Series<TimestampPair> fillNullsBackwards() {
               return null;
           }

           @Override
           public Series<TimestampPair> fillNullsForward() {
               return null;
           }


       };
    }


}
