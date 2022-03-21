
package io.github.dflib.core.parser.filter;

import io.github.dflib.core.parser.CompileErrorListener;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.query.ExpLexer;
import io.github.dflib.query.ExpParser;
import io.github.dflib.query.filter.Filter;
import io.vavr.control.Option;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;

public class FilterCompiler {

    public static Option<Filter> compile(String expression) {
        expression = expression.trim();
        if (expression.isEmpty()) {
            throw new CompileException("expression is null");
        }
        CompileErrorListener errorListener = new CompileErrorListener(expression);
        CharStream stream = CharStreams.fromString(expression);
        ExpLexer lexer = new ExpLexer(stream);
        lexer.removeErrorListener(ConsoleErrorListener.INSTANCE);
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExpParser parser = new ExpParser(tokens);
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);
        parser.addErrorListener(errorListener);
        FilterExpVisitor filterExpVisitor = new FilterExpVisitor();
        Object filter = filterExpVisitor.visit(parser.compilationUnit());
        if (filter instanceof Filter) {
            return Option.of((Filter) filter);
        } else {
            throw new CompileException("expression is not a valid filter:" + expression);
        }
    }

}
