package io.github.dflib.core;

public interface UserDefinedFunction1<T, R> extends UserDefinedFunction {

    R eval(T param);
}
