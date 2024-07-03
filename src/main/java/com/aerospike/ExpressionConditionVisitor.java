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
    public Expression visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        Exp exp = Exp.expr(visit(ctx.expression()));
        return Exp.build(Exp.not(exp));
    }

    @Override
    public Expression visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.getChild(0)));
        Exp right = Exp.expr(visit(ctx.getChild(2)));

        return Exp.build(Exp.gt(left, right));
    }

    @Override
    public Expression visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.getChild(0)));
        Exp right = Exp.expr(visit(ctx.getChild(2)));

        return Exp.build(Exp.ge(left, right));
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
        Exp left = Exp.expr(visit(ctx.getChild(0)));
        Exp right = Exp.expr(visit(ctx.getChild(2)));

        return Exp.build(Exp.lt(left, right));
    }

    @Override
    public Expression visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.getChild(0)));
        Exp right = Exp.expr(visit(ctx.getChild(2)));

        return Exp.build(Exp.le(left, right));
    }

    @Override
    public Expression visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.getChild(0)));
        Exp right = Exp.expr(visit(ctx.getChild(2)));

        return Exp.build(Exp.eq(left, right));
    }

    @Override
    public Expression visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        Exp left = Exp.expr(visit(ctx.getChild(0)));
        Exp right = Exp.expr(visit(ctx.getChild(2)));

        return Exp.build(Exp.ne(left, right));
    }

    @Override
    public Expression visitMetadata(ConditionParser.MetadataContext ctx) {
        String functionName;
        Integer optionalParam = null;
        if (ctx.metadataFunction() == null) {
            functionName = ctx.digestModulo().getText();
            optionalParam = Integer.valueOf(ctx.NUMBER().getText());
        } else {
            functionName = ctx.metadataFunction().getText();
        }
        return Exp.build(visitFunctionName(functionName, optionalParam));
    }

    @Override
    public Expression visitMetadataFunction(ConditionParser.MetadataFunctionContext ctx) {
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

    @Override
    public Expression visitPath(ConditionParser.PathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitQuotedString(ConditionParser.QuotedStringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitNumber(ConditionParser.NumberContext ctx) {
        String text = ctx.getText();
        Exp val;
        if (isInQuotes(text)) {
            val = Exp.val(getWithoutQuotes(text));
        } else {
            val = Exp.val(Long.parseLong(text));
        }
        return Exp.build(val);
    }

    @Override
    protected Expression aggregateResult(Expression aggregate, Expression nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
