package io.github.dflib.dflib.exp;

import com.nhl.dflib.Condition;
import com.nhl.dflib.StrExp;
import com.nhl.dflib.exp.map.MapExpScalarCondition2;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;


public interface MyStrExp extends StrExp {

    default Condition in(String[] values) {
        return MapExpScalarCondition2.mapVal("in", this, values, (s, r) -> ArrayUtils.contains(r, s));
    }

    default Condition notIn(String[] values) {
        return MapExpScalarCondition2.mapVal("not in", this, values, (s, r) -> Arrays.stream(r).noneMatch(it-> it.equals(s)));
    }

    default Condition contains(String value) {
        return MapExpScalarCondition2.mapVal("contain", this, value, (s, p) -> s.contains(value));
    }
}
