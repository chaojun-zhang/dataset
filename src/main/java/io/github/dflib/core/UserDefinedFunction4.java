package io.github.dflib.core;

public interface UserDefinedFunction4<T1, T2, T3, T4, R> extends UserDefinedFunction {

    R eval(T1 param1, T2 param2, T3 param3, T4 parma4);
}
