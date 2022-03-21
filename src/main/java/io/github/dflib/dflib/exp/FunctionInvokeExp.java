package io.github.dflib.dflib.exp;

import com.nhl.dflib.Exp;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.exp.ExpN;
import io.github.dflib.core.UserDefinedFunction;
import io.github.dflib.core.parser.ParserContext;
import io.github.dflib.dflib.AccumulatorFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

public class FunctionInvokeExp extends ExpN {

    private final Class<UserDefinedFunction> udfClazz;

    private final ParserContext parserContext;

    private final Method method;


    public FunctionInvokeExp(String opName, Class<UserDefinedFunction> udfClazz,
                             ParserContext context,Method method, Exp<?>... args) {
        super(opName, Objects.requireNonNull(method).getReturnType(), args);
        this.method = method;
        this.udfClazz = udfClazz;
        this.parserContext = context;
    }


    @Override
    protected Series doEval(int height, Series[] args) {
        try {
            try (UserDefinedFunction userDefinedFunction = udfClazz.getDeclaredConstructor().newInstance()) {
                userDefinedFunction.open(parserContext);
                Accumulator accumulator = AccumulatorFactory.get(getType());
                int size = args[0].size();
                for (int i = 0; i < size; i++) {
                    int finalI = i;
                    Object[] objects = Arrays.stream(args).map(it -> it.get(finalI)).toArray(Object[]::new);
                    Object result = method.invoke(userDefinedFunction, objects);
                    accumulator.add(result);
                }
                return accumulator.toSeries();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
