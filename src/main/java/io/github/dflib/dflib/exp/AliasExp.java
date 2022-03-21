package io.github.dflib.dflib.exp;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Exp;
import com.nhl.dflib.exp.AsExp;

public class AliasExp extends AsExp {
    public AliasExp(String name, Exp delegate) {
        super(name, delegate);
    }

    @Override
    public String toQL() {
        return getColumnName();
    }

    @Override
    public String toQL(DataFrame df) {
        return getColumnName(df);
    }
}
