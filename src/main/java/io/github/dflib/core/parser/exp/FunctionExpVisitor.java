package io.github.dflib.core.parser.exp;


import com.nhl.dflib.Exp;
import com.nhl.dflib.exp.agg.DoubleExpAggregator;
import com.nhl.dflib.exp.num.NumericExpFactory;
import io.github.dflib.core.UserDefinedFunction;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.dataframe.Field;
import io.github.dflib.dataframe.aggregator.PercentileAccumulator;
import io.github.dflib.dflib.exp.FunctionInvokeExp;
import io.github.dflib.dflib.exp.TimestampPair;
import io.github.dflib.dflib.exp.TimestampPairExp;
import io.github.dflib.query.AggregateType;
import io.github.dflib.query.ExpBaseVisitor;
import io.github.dflib.query.ExpParser;


import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;


public class FunctionExpVisitor extends ExpBaseVisitor<Exp<?>> {

    private final ExpVisitor expVisitor;

    public FunctionExpVisitor(ExpVisitor expVisitor) {
        this.expVisitor = expVisitor;
    }

    @Override
    public Exp<?> visitFunctionExpr(ExpParser.FunctionExprContext ctx) {
        String funcName = ctx.identifier().getText();
        ExpParser.FnArgsContext fnArgsContext = ctx.fnArgs();
        if (AggregateType.SUM.getName().equalsIgnoreCase(funcName)) {
            Exp<? extends Number> numExp = numExp(expVisitor, fnArgsContext.expr(0));
            return NumericExpFactory.factory(numExp).sum(numExp);
        } else if (AggregateType.AVG.getName().equalsIgnoreCase(funcName)) {
            Exp<? extends Number> numExp = numExp(expVisitor, fnArgsContext.expr(0));
            return NumericExpFactory.factory(numExp).avg(numExp);
        } else if (AggregateType.MAX.getName().equalsIgnoreCase(funcName)) {
            Exp<? extends Number> numExp = numExp(expVisitor, fnArgsContext.expr(0));
            return NumericExpFactory.factory(numExp).max(numExp);
        } else if (AggregateType.MIN.getName().equalsIgnoreCase(funcName)) {
            Exp<? extends Number> numExp = numExp(expVisitor, fnArgsContext.expr(0));
            return NumericExpFactory.factory(numExp).min(numExp);
        } else if (AggregateType.COUNT.getName().equalsIgnoreCase(funcName)) {
            return Exp.count();
        } else {
            Optional<AggregateType> aggregateType = AggregateType.from(funcName);
            if (aggregateType.isPresent() && !aggregateType.get().isNormal()) {//峰值，95聚合函数
                return buildAggExpr(fnArgsContext, aggregateType);
            } else {
                //udf goes here
                return buildUdfExpr(funcName, fnArgsContext);
            }
        }
    }

    private DoubleExpAggregator<TimestampPair> buildAggExpr(ExpParser.FnArgsContext fnArgsContext, Optional<AggregateType> aggregateType) {
        Optional<Field> eventTimeField = expVisitor.getParserContext().getSchema().getEventTimeField();
        if (eventTimeField.isPresent()) {
            Exp<? extends Number> metricExpr = numExp(expVisitor, fnArgsContext.expr(0));
            TimestampPairExp pairExp = new TimestampPairExp("xx", eventTimeField.get().toExpr(), metricExpr);
            PercentileAccumulator intPercentileAgg = new PercentileAccumulator(aggregateType.get(), expVisitor.getParserContext().getInterval());
            return new DoubleExpAggregator<TimestampPair>("long", pairExp, intPercentileAgg::agg);
        } else {
            throw new CompileException("no datetime field found in source");
        }
    }

    /**
     * 针对95峰值等聚合表达式
     *
     * @param funcName
     * @return
     */
    private FunctionInvokeExp buildUdfExpr(String funcName, ExpParser.FnArgsContext fnArgsContext) {
        try {
            Exp<?>[] exps = fnArgsContext.expr().stream().map(it -> expVisitor.visit(it)).toArray(Exp<?>[]::new);
            String functionClassName = expVisitor.getParserContext().getFunctions().get(funcName);
            Class<UserDefinedFunction> functionClass = (Class<UserDefinedFunction>) Class.forName(functionClassName);
            Class[] paramTypes = Arrays.stream(exps).map(Exp::getType).toArray(Class[]::new);
            Method udfMethod = functionClass.getMethod(UserDefinedFunction.FUNC_NAME, paramTypes);
            return new FunctionInvokeExp(funcName, functionClass, expVisitor.getParserContext(), udfMethod, exps);
        } catch (Exception e) {
            throw new CompileException("Fail to parse function, func: " + funcName, e);
        }
    }

    private Exp<? extends Number> numExp(ExpVisitor expVisitor, ExpParser.ExprContext expr) {
        Exp<?> exp = expVisitor.visit(expr);
        if (Number.class.isAssignableFrom(exp.getType())) {
            return (Exp<? extends Number>) exp;
        } else {
            throw new CompileException("only support for num exp");
        }
    }


}
