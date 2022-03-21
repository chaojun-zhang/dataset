package io.github.dflib.core;

public interface UserDefinedFunction2<T1, T2, R> extends UserDefinedFunction {

    R eval(T1 param1, T2 param2);
}
