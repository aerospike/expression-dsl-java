package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

import java.util.Objects;
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
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, should there be support for byte[]?
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::gt);

        return Exp.build(expr);
    }

    private Exp getSimpleComparisonExpr(String leftOperandText, Object rightOperand, BinaryOperator<Exp> operator) {
        Exp right;
        Exp.Type binType;

        // set Exp value type and bin type based on right operand
        if (Objects.requireNonNull(rightOperand) instanceof String str) {
            if (isInQuotes(str)) {
                right = Exp.val(getWithoutQuotes(str));
                binType = Exp.Type.STRING;
            } else {
                right = Exp.val(Long.parseLong(str));
                binType = Exp.Type.INT;
            }
        } else {
            throw new UnsupportedOperationException("Unexpected right operand type: " + rightOperand);
        }
        return operator.apply(Exp.bin(leftOperandText.replace("$.", ""), binType), right);
    }

    private boolean isInQuotes(String str) {
        return (str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'"));
    }

    private String getWithoutQuotes(String str) {
        if (str.length() > 2) {
            return str.substring(1, str.length() - 1);
        } else {
            throw new IllegalArgumentException(String.format("String %s must contain more than 2 characters", str));
        }
    }

    @Override
    public Expression visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, should there be support for byte[]?
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::lt);

        return Exp.build(expr);
    }

    @Override
    public Expression visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, should there be support for byte[]?
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::eq);

        return Exp.build(expr);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     */
    @Override
    public Expression visitFunctionName(ConditionParser.FunctionNameContext ctx) {
        String functionName = ctx.getChild(0).getText();
        return switch (functionName) {
            case "deviceSize" -> Exp.build(Exp.deviceSize());
            case "ttl" -> Exp.build(Exp.ttl());
            case "exists" -> Exp.build(Exp.binExists("test")); // TODO: get the preceding path
            default -> throw new IllegalArgumentException("Unknown function: " + functionName);
        };
    }

    @Override
    public Expression visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }
//
//    @Override
//    public Expression visitFunctionCall(ConditionParser.FunctionCallContext ctx) {
//        String functionName = ctx.functionName().getText();
//        return switch (functionName) {
//            case "deviceSize" -> Exp.build(Exp.deviceSize());
//            case "ttl" -> Exp.build(Exp.ttl());
//            default -> throw new IllegalArgumentException("Unknown function: " + functionName);
//        };
//    }

    // visitNUMBER and NUMBERContext doesn't exists?
    //@Override
    //public Expression visitNUMBER(ConditionParser.NUMBERContext ctx) {
    //    return Exp.val(Integer.parseInt(ctx.getText()));
    //}
}
