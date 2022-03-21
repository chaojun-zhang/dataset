package io.github.dflib.core;

import io.github.dflib.core.parser.ParserContext;

import java.io.Closeable;
import java.io.IOException;

public interface UserDefinedFunction extends Closeable {

    String FUNC_NAME = "eval";

    default void open(ParserContext context) {
    }

    default void close() throws IOException {
    }



}
