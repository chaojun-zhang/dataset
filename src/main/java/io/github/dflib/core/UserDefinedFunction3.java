package io.github.dflib.core;

public interface UserDefinedFunction3<T1, T2, T3, R> extends UserDefinedFunction {

    R eval(T1 param1, T2 param2, T3 param3);
}
