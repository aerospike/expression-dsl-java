package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

import java.util.function.BinaryOperator;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<Expression> {

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
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, must be not just String
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::gt);

        return Exp.build(expr);
    }

    private Exp getSimpleComparisonExpr(String leftOperandText, Object rightOperand, BinaryOperator<Exp> operator) {
        Exp right = null;
        Exp.Type binType;

        // set Exp value type and bin type based on right operand
        switch (rightOperand) {
            case String str -> {
                if (isInQuotes(str)) {
                    right = Exp.val((String) rightOperand);
                    binType = Exp.Type.STRING;
                } else {
                    right = Exp.val(Long.parseLong((String) rightOperand));
                    binType = Exp.Type.INT;
                }
            }
            default -> throw new UnsupportedOperationException("Unexpected right operand type: " + rightOperand);
        }
        return operator.apply(Exp.bin(leftOperandText.replace("$.", ""), binType), right);
    }

    private Exp getRightExp() {
        return null;
    }

    private boolean isInQuotes(String str) {
        return (str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'"));
    }

    @Override
    public Expression visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.operand(0)));
        Exp right = Exp.expr(visit(ctx.operand(1)));
        return Exp.build(Exp.lt(left, right));
    }

    @Override
    public Expression visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, must be not just String
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::eq);

        return Exp.build(expr);
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
