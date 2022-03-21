package io.github.dflib.dflib.column;

import com.nhl.dflib.exp.GenericColumn;
import io.github.dflib.dflib.exp.InstantExp;

public class InstantColumn extends GenericColumn<Long> implements InstantExp {

    public InstantColumn(String name) {
        super(name, Long.class);
    }

    public InstantColumn(int position) {
        super(position, Long.class);
    }

    @Override
    public String toQL() {
        return position >= 0 ? "$Instant(" + position + ")" : name;
    }
}
