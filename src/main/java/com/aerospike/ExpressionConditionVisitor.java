package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

import java.util.Objects;
import java.util.function.BinaryOperator;

import static com.aerospike.util.MetadataParsingUtils.*;
import static com.aerospike.util.ParsingUtils.getWithoutQuotes;
import static com.aerospike.util.ParsingUtils.isInQuotes;

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

    @Override
    public Expression visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, should there be support for byte[]?
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::ge);

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

        // Handle Record Metadata expressions
        if (isMetadataExpression(leftOperandText)) {
            // For cases like digestModulo(<int>) when a metadata expression accepts a parameter
            if (metadataContainsParameter(leftOperandText)) {
                return operator.apply(
                        visitFunctionName(
                                extractMetadataExpression(leftOperandText),
                                Integer.parseInt(Objects.requireNonNull(extractParameterMetadataExpression(leftOperandText)))
                        ), right
                );
            }
            return operator.apply(
                    visitFunctionName(
                            extractMetadataExpression(leftOperandText), null
                    ), right
            );
        }
        // Handle Bin expressions
        return operator.apply(
                Exp.bin(
                        leftOperandText.replace("$.", ""), binType
                ), right
        );
    }

    @Override
    public Expression visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, should there be support for byte[]?
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::lt);

        return Exp.build(expr);
    }

    @Override
    public Expression visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        Object rightOperand = ctx.getChild(2).getText(); // TODO: temp, should there be support for byte[]?
        Exp expr = getSimpleComparisonExpr(ctx.getChild(0).getText(), rightOperand, Exp::le);

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
        return Exp.build(visitFunctionName(functionName, null));
    }

    private Exp visitFunctionName(String functionName, Integer optionalParam) {
        return switch (functionName) {
            case "deviceSize" -> Exp.deviceSize();
            case "memorySize" -> Exp.memorySize();
            case "recordSize" -> Exp.recordSize();
            case "digestModulo" -> Exp.digestModulo(optionalParam);
            case "isTombstone" -> Exp.isTombstone();
            case "keyExists" -> Exp.keyExists();
            case "lastUpdate" -> Exp.lastUpdate();
            case "sinceUpdate" -> Exp.sinceUpdate();
            case "setName" -> Exp.setName();
            case "ttl" -> Exp.ttl();
            case "voidTime" -> Exp.voidTime();
            // TODO: exists doesn't belong here, this is metadata function name?
            case "exists" -> Exp.binExists("test"); // TODO: get the preceding path
            default -> throw new IllegalArgumentException("Unknown function: " + functionName);
        };
    }

    @Override
    public Expression visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }

    /**
     * Aggregates the results of visiting multiple children of a node. After
     * either all children are visited or {@link #shouldVisitNextChild} returns
     * {@code false}, the aggregate value is returned as the result of
     * {@link #visitChildren}.
     *
     * <p>The default implementation returns {@code nextResult}, meaning
     * {@link #visitChildren} will return the result of the last child visited
     * (or return the initial value if the node has no children).</p>
     *
     * @param aggregate  The previous aggregate value. In the default
     *                   implementation, the aggregate value is initialized to
     *                   {@link #defaultResult}, which is passed as the {@code aggregate} argument
     *                   to this method after the first child node is visited.
     * @param nextResult The result of the immediately preceeding call to visit
     *                   a child node.
     * @return The updated aggregate result.
     */
    @Override
    protected Expression aggregateResult(Expression aggregate, Expression nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
