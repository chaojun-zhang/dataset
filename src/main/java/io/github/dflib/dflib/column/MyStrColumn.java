package io.github.dflib.dflib.column;

import com.nhl.dflib.exp.str.StrColumn;
import io.github.dflib.dflib.exp.MyStrExp;

public class MyStrColumn extends StrColumn implements MyStrExp {
    public MyStrColumn(String name) {
        super(name);
    }

    public MyStrColumn(int position) {
        super(position);
    }
}
