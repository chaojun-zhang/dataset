package io.github.dflib.core.parser.exp;

import com.nhl.dflib.Condition;
import com.nhl.dflib.Exp;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.core.parser.ParserContext;
import io.github.dflib.dataframe.Field;
import io.github.dflib.dflib.exp.AliasExp;
import io.github.dflib.dflib.exp.CaseWhenExp;
import io.github.dflib.dflib.exp.MyIfExp;
import io.github.dflib.query.ExpBaseVisitor;
import io.github.dflib.query.ExpParser;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ExpVisitor extends ExpBaseVisitor<Exp<?>> {

    @Getter
    private final ParserContext parserContext;

    public ExpVisitor(ParserContext parserContext) {
        this.parserContext = parserContext;
    }


    @Override
    public Exp<?> visitNestedExpr(ExpParser.NestedExprContext ctx) {
        return this.visit(ctx.expr());
    }

    @Override
    public Exp<?> visitNamedExpr(ExpParser.NamedExprContext ctx) {
        Exp<?> expr = this.visit(ctx.expr());
        String alias = ctx.identifier().getText();
        return new AliasExp(alias, expr);
    }

    @Override
    public Exp<?> visitPredicateExpr(ExpParser.PredicateExprContext ctx) {
        ConditionExpVisitor predicateExpVisitor = new ConditionExpVisitor(this);
        return predicateExpVisitor.visitPredicateExpr(ctx);
    }

    @Override
    public Exp<?> visitLongLiteral(ExpParser.LongLiteralContext ctx) {
        return Exp.$val(Long.valueOf(ctx.getText()), Long.class);
    }

    @Override
    public Exp<?> visitDoubleLiteral(ExpParser.DoubleLiteralContext ctx) {
        return Exp.$val(Double.valueOf(ctx.getText()), Double.class);
    }

    @Override
    public Exp<?> visitStringLiteral(ExpParser.StringLiteralContext ctx) {
        //字符串需要去除前后缀
        String stringElement = ctx.stringElement().getText();
        String text = StringUtils.substring(stringElement, 1, stringElement.length() - 1);
        return Exp.$val(text, String.class);
    }


    @Override
    public Exp<?> visitMulDivModuloExpr(ExpParser.MulDivModuloExprContext ctx) {
        ArithmeticExpVisitor arithmeticExpVisitor = new ArithmeticExpVisitor(this);
        return arithmeticExpVisitor.visitMulDivModuloExpr(ctx);

    }

    @Override
    public Exp<?> visitAddSubExpr(ExpParser.AddSubExprContext ctx) {
        ArithmeticExpVisitor arithmeticExpVisitor = new ArithmeticExpVisitor(this);
        return arithmeticExpVisitor.visitAddSubExpr(ctx);
    }

    @Override
    public Exp<?> visitFunctionExpr(ExpParser.FunctionExprContext ctx) {
        FunctionExpVisitor functionExpVisitor = new FunctionExpVisitor(this);
        return functionExpVisitor.visitFunctionExpr(ctx);
    }

    @Override
    public Exp<?> visitColumn(ExpParser.ColumnContext ctx) {
        String colName = ctx.IDENTIFIER().getText();
        Field field = parserContext.getSchema().findField(colName).orElseThrow(() -> new CompileException("field " + colName + " not found in source schema"));
        return field.toExpr();
    }

    @Override
    public Exp<?> visitIfExpr(ExpParser.IfExprContext ctx) {
        Condition condition = (Condition) this.visit(ctx.predicate());
        Exp<?> ifTrueExp = this.visit(ctx.expr(0));
        Exp<?> ifFalseExp = this.visit(ctx.expr(1));
        return new MyIfExp(condition, ifTrueExp, ifFalseExp);
    }

    @Override
    public Exp<?> visitCaseWhenExpr(ExpParser.CaseWhenExprContext ctx) {
        final List<Condition> conditionList = new ArrayList<>();
        final List<Exp<?>> expList = new ArrayList<>();
        for (ExpParser.WhenExprContext whenExprContext : ctx.whenExpr()) {
           Condition predicate = (Condition) this.visit(whenExprContext.predicate());
            conditionList.add(predicate);
            Exp<?> exp = this.visit(whenExprContext.expr());
            expList.add(exp);
        }
        final Exp<?> alternative = this.visit(ctx.alternative);
        return new CaseWhenExp(conditionList, expList, alternative);
    }
}
