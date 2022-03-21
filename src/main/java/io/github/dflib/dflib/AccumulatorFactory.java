package io.github.dflib.dflib;

import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.accumulator.DoubleAccumulator;
import com.nhl.dflib.accumulator.IntAccumulator;
import com.nhl.dflib.accumulator.LongAccumulator;
import com.nhl.dflib.accumulator.ObjectAccumulator;
import io.github.dflib.dataframe.FieldType;
import io.github.dflib.dflib.accumulator.InstantAccumulator;

import java.sql.Timestamp;

public interface AccumulatorFactory {

    static Accumulator<?> get(Class<?> type){
        return get(type, 10);
    }

    static Accumulator<?> get(Class<?> type,int capacity){
        if (Integer.TYPE == type || Integer.class == type || int.class == type) {
            return new IntAccumulator(capacity);
        } else if (Long.TYPE == type || Long.class == type || long.class == type) {
            return new LongAccumulator(capacity);
        } else if (Double.TYPE == type || Double.class == type || double.class == type) {
            return new DoubleAccumulator(capacity);
        } else if (Timestamp.class == type) {
            return new InstantAccumulator(capacity);
        } else {
            return new ObjectAccumulator<>(capacity);
        }
    }

    static Accumulator<?> get(FieldType type) {
        if (FieldType.INSTANT == type) {
            return new InstantAccumulator();
        } else if (FieldType.INT == type) {
            return new IntAccumulator();
        } else if (FieldType.LONG == type) {
            return new LongAccumulator();
        } else if (FieldType.DOUBLE == type) {
            return new DoubleAccumulator();
        } else {
            return new ObjectAccumulator<>();
        }
    }
}
