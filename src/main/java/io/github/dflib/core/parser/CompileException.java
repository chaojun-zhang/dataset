
package io.github.dflib.core.parser;


public class CompileException extends RuntimeException {
    public CompileException(String message) {
        super(message);
    }

    public CompileException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
