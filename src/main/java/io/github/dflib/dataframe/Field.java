package io.github.dflib.dataframe;

import com.nhl.dflib.Exp;
import com.nhl.dflib.accumulator.Accumulator;
import io.github.dflib.dflib.AccumulatorFactory;
import io.github.dflib.dflib.column.MyStrColumn;
import io.github.dflib.dflib.exp.InstantExp;
import lombok.Getter;

@Getter
public class Field {

    private final String name;
    private final FieldType type;

    public Field(String name, FieldType type) {
        this.name = name;
        this.type = type;
    }

    public boolean isMetricField() {
        return type == FieldType.DOUBLE || type == FieldType.LONG || type == FieldType.INT;
    }

    public boolean isDimensionField() {
        return type == FieldType.STRING;
    }

    public boolean isEventTimeField() {
        return type == FieldType.INSTANT;
    }

    public Accumulator accumulator() {
        return AccumulatorFactory.get(this.type);
    }


    public static Field $int(String name) {
        return new Field(name, FieldType.INT);
    }

    public static Field $long(String name) {
        return new Field(name, FieldType.LONG);
    }
    public static Field $double(String name) {
        return new Field(name, FieldType.DOUBLE);
    }

    public static Field $str(String name) {
        return new Field(name, FieldType.STRING);
    }
    public static Field $datetime(String name) {
        return new Field(name, FieldType.INSTANT);
    }

    public Exp<?> toExpr(){
        if (FieldType.DOUBLE == type) {
            return Exp.$double(name);
        } else if (FieldType.LONG == type) {
            return Exp.$long(name);
        } else if (FieldType.INT == type) {
            return Exp.$int(name);
        } else if (FieldType.INSTANT == type) {
            return InstantExp.$datetime(name);
        } else {
            return new MyStrColumn(name);
        }
    }
}
