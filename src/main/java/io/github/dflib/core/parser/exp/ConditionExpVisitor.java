package io.github.dflib.core.parser.exp;

import com.nhl.dflib.Condition;
import com.nhl.dflib.Exp;
import com.nhl.dflib.StrExp;
import com.nhl.dflib.exp.num.NumericExpFactory;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.dflib.exp.MyStrExp;
import io.github.dflib.query.ExpBaseVisitor;
import io.github.dflib.query.ExpParser;
import org.antlr.v4.runtime.tree.ParseTree;

public class ConditionExpVisitor extends ExpBaseVisitor<Condition> {

    private final ExpVisitor expVisitor;

    public ConditionExpVisitor(ExpVisitor expVisitor) {
        this.expVisitor = expVisitor;
    }

    @Override
    public Condition visitPredicateAndOrExpr(ExpParser.PredicateAndOrExprContext ctx) {
        Condition leftCond = this.visit(ctx.predicate(0));
        Condition rightCond = this.visit(ctx.predicate(1));
        if (ctx.AND()!=null) {
            return leftCond.and(rightCond);
        } else {
            return leftCond.or(rightCond);
        }
    }

    @Override
    public Condition visitNestedPredicateExpr(ExpParser.NestedPredicateExprContext ctx) {
        Condition condition = this.visit(ctx.predicate());

        if (ctx.NEGATE() != null) {
            return condition.not();
        }
        return condition;
    }

    @Override
    public Condition visitNestedFieldPredicate(ExpParser.NestedFieldPredicateContext ctx) {
        Condition condition = this.visit(ctx.fieldPredicate());
        if (ctx.NEGATE() != null) {
            return condition.not();
        }
        return condition;
    }

    @Override
    public Condition visitRegFieldPredicate(ExpParser.RegFieldPredicateContext ctx) {
        Exp<?> exp = expVisitor.visit(ctx.expr());
        if (exp instanceof StrExp) {
            StrExp strExp = (StrExp) exp;
            String regex = ctx.REGEX().getText();
            if (!regex.startsWith("/") || !regex.endsWith("/")) {
                throw new CompileException(String.format("wrong pattern format: %s", regex));
            }
            String pattern = regex.substring(1, regex.length() - 1);
            return strExp.matches(pattern);
        } else {
            throw new CompileException("reg predicate only support for StringExp, ctx:" + ctx);
        }
    }

    @Override
    public Condition visitStrInFieldPredicate(ExpParser.StrInFieldPredicateContext ctx) {
        Exp<?> exp = expVisitor.visit(ctx.expr());

        if (exp instanceof MyStrExp) {
            MyStrExp strExp = (MyStrExp) exp;
            String[] stringArray = ctx.stringArray().stringElement().stream()
                    .map(ParseTree::getText)
                    .map(it -> it.substring(1, it.length() - 1))
                    .toArray(String[]::new);
            if (ctx.NOT() != null) {
                return strExp.notIn(stringArray);
            } else {
                return strExp.in(stringArray);
            }
        } else {
            throw new CompileException("reg predicate only support for StringExp, ctx:" + ctx);
        }
    }

    @Override
    public Condition visitOpFieldPredicate(ExpParser.OpFieldPredicateContext ctx) {
        Exp<?> leftExp = expVisitor.visit(ctx.expr(0));
        Exp<?> rightExp = expVisitor.visit(ctx.expr(1));
        if (Number.class.isAssignableFrom(leftExp.getType()) && Number.class.isAssignableFrom(rightExp.getType())) {
            Exp<? extends Number> leftNum = (Exp<? extends Number>) leftExp;
            Exp<? extends Number> rightNum = (Exp<? extends Number>) rightExp;
            if (ctx.GT() != null) {
                return NumericExpFactory.factory(leftNum, rightNum).gt(leftNum, rightNum);
            } else if (ctx.GEQ() != null) {
                return NumericExpFactory.factory(leftNum, rightNum).ge(leftNum, rightNum);
            } else if (ctx.LT() != null) {
                return NumericExpFactory.factory(leftNum, rightNum).lt(leftNum, rightNum);
            } else if (ctx.LEQ() != null) {
                return NumericExpFactory.factory(leftNum, rightNum).le(leftNum, rightNum);
            } else if (ctx.EQ() != null) {
                return NumericExpFactory.factory(leftNum, rightNum).eq(leftNum, rightNum);
            } else if (ctx.NEQ() != null) {
                return NumericExpFactory.factory(leftNum, rightNum).ne(leftNum, rightNum);
            }
        } else {
            if (ctx.EQ() != null) {
                return leftExp.eq(rightExp);
            } else if (ctx.NEQ() != null) {
                return leftExp.ne(rightExp);
            } else if (ctx.LIKE() != null && leftExp instanceof MyStrExp) {
                MyStrExp leftStrExp = (MyStrExp) leftExp;
                String value = ctx.expr(1).getText();
                if (value.startsWith("'") || value.startsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                boolean startWith = value.startsWith("%");
                boolean endWith = value.endsWith("%");
                if (startWith && endWith) {
                    return leftStrExp.contains(value.substring(1, value.length() - 1));
                } else if (startWith) {
                    return leftStrExp.endsWith(value.substring(1));
                } else if (endWith) {
                    return leftStrExp.startsWith(value.substring(0, value.length() - 1));
                }
            }
        }
        throw new CompileException("unsupported op, ctx:" + ctx.getText());
    }

}
