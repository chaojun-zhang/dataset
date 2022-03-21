package io.github.dflib.core.parser.exp;

import com.nhl.dflib.Exp;
import com.nhl.dflib.StrExp;
import com.nhl.dflib.exp.num.NumericExpFactory;
import com.nhl.dflib.exp.str.ConcatExp;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.query.ExpBaseVisitor;
import io.github.dflib.query.ExpParser;

public class ArithmeticExpVisitor extends ExpBaseVisitor<Exp> {

    private final ExpVisitor visitor;

    public ArithmeticExpVisitor(ExpVisitor visitor) {
        this.visitor = visitor;
    }


    @Override
    public Exp visitAddSubExpr(ExpParser.AddSubExprContext ctx) {
        Exp<?> leftExp = visitor.visit(ctx.expr(0));
        Exp<?> rightExp = visitor.visit(ctx.expr(1));

        if (Number.class.isAssignableFrom(leftExp.getType()) && Number.class.isAssignableFrom(rightExp.getType())) {
            Exp<? extends Number> leftNum = (Exp<? extends Number>) leftExp;
            Exp<? extends Number> rightNum = (Exp<? extends Number>) rightExp;
            NumericExpFactory factory = NumericExpFactory.factory(leftNum, rightNum);
            if (ctx.PLUS() != null) {
                return factory.add(leftNum, rightNum);
            } else if (ctx.MINUS() != null) {
                return factory.sub(leftNum, rightNum);
            }
        } else if (leftExp instanceof StrExp || rightExp instanceof StrExp
                || leftExp.getType() == String.class || rightExp.getType() == String.class) {
            if (ctx.PLUS() != null) {
                return ConcatExp.forObjects(leftExp, rightExp);
            }
        }
        throw new CompileException("operation is not supported, op: " + ctx.getText());
    }

    @Override
    public Exp visitMulDivModuloExpr(ExpParser.MulDivModuloExprContext ctx) {

        Exp<?> leftExp = visitor.visit(ctx.expr(0));
        Exp<?> rightExp = visitor.visit(ctx.expr(1));

        if (Number.class.isAssignableFrom(leftExp.getType()) && Number.class.isAssignableFrom(rightExp.getType())) {
            Exp<? extends Number> leftNum = (Exp<? extends Number>) leftExp;
            Exp<? extends Number> rightNum = (Exp<? extends Number>) rightExp;
            NumericExpFactory factory = NumericExpFactory.factory(leftNum, rightNum);
            if (ctx.DIV() != null) {
                return factory.div(leftNum, rightNum);
            } else if (ctx.MUL() != null) {
                return factory.mul(leftNum, rightNum);
            } else if (ctx.MODULO() != null) {
                return factory.mod(leftNum, rightNum);
            }
        }
        throw new CompileException("operation is not supported, op: " + ctx.getText());

    }
}
