package io.github.dflib.core.parser.exp;

import com.nhl.dflib.Condition;
import com.nhl.dflib.Exp;
import io.github.dflib.core.parser.CompileErrorListener;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.core.parser.ParserContext;
import io.github.dflib.query.ExpLexer;
import io.github.dflib.query.ExpParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;

public class ExpCompiler {

    public static Exp<?> compile(String expression, ParserContext compileContext) {
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
        ExpVisitor expVisitor = new ExpVisitor(compileContext);
        return expVisitor.visit(parser.compilationUnit());
    }

    public static Condition compileCondition(String expression, ParserContext compileContext) {
        return (Condition) compile(expression, compileContext);
    }

}
