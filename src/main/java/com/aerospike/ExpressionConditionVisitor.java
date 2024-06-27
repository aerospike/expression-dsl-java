package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

public class ExpressionConditionVisitor extends com.aerospike.ConditionBaseVisitor<Expression> {

    @Override
    public Expression visitAndExpression(ConditionParser.AndExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.expression(0)));
        Exp right = Exp.expr(visit(ctx.expression(1)));
        return Exp.build(Exp.and(left, right));
    }

    @Override
    public Expression visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.expression(0)));
        Exp right = Exp.expr(visit(ctx.expression(1)));
        return Exp.build(Exp.or(left, right));
    }

    @Override
    public Expression visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.operand(0)));
        Exp right = Exp.expr(visit(ctx.operand(1)));
        return Exp.build(Exp.gt(left, right));
    }

    @Override
    public Expression visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.operand(0)));
        Exp right = Exp.expr(visit(ctx.operand(1)));
        return Exp.build(Exp.lt(left, right));
    }

    @Override
    public Expression visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }

    @Override
    public Expression visitFunctionCall(ConditionParser.FunctionCallContext ctx) {
        String functionName = ctx.functionName().getText();
        return switch (functionName) {
            case "deviceSize" -> Exp.build(Exp.deviceSize());
            case "ttl" -> Exp.build(Exp.ttl());
            default -> throw new IllegalArgumentException("Unknown function: " + functionName);
        };
    }

    // visitNUMBER and NUMBERContext doesn't exists?
    //@Override
    //public Expression visitNUMBER(ConditionParser.NUMBERContext ctx) {
    //    return Exp.val(Integer.parseInt(ctx.getText()));
    //}
}
