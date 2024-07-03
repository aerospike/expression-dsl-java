package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.expSource.*;

import java.util.function.BinaryOperator;

import static com.aerospike.util.ParsingUtils.getWithoutQuotes;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<ExpSource> {

    @Override
    public ExpSource visitAndExpression(ConditionParser.AndExpressionContext ctx) {
        ExpSource left = visit(ctx.expression(0));
        ExpSource right = visit(ctx.expression(1));

        return new Expr(Exp.and(left.getExp(), right.getExp()));
    }

    @Override
    public ExpSource visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        ExpSource left = visit(ctx.expression(0));
        ExpSource right = visit(ctx.expression(1));

        return new Expr(Exp.or(left.getExp(), right.getExp()));
    }

    @Override
    public ExpSource visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        Exp exp = visit(ctx.expression()).getExp();

        return new Expr(Exp.not(exp));
    }

    @Override
    public ExpSource visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        ExpSource left = visit(ctx.operand(0));
        ExpSource right = visit(ctx.operand(1));

        Exp exp = getExp(left, right, Exp::gt);
        return new Expr(exp);
    }

    private Exp getExp(ExpSource left, ExpSource right, BinaryOperator<Exp> operator) {
        String binName;
        Exp exp = null;

        if (left == null) {
            throw new IllegalArgumentException("Cannot parse left operand");
        }
        if (right == null) {
            throw new IllegalArgumentException("Cannot parse right operand");
        }

        if (left.getType() == ExpSource.Type.BIN_OPERAND) {
            binName = left.getBinName();
            if (right.getType() == ExpSource.Type.NUMBER_OPERAND) {
                exp = operator.apply(Exp.bin(binName, Exp.Type.INT), Exp.val(right.getNumber()));
            }
            if (right.getType() == ExpSource.Type.STRING_OPERAND) {
                exp = operator.apply(Exp.bin(binName, Exp.Type.STRING), Exp.val(right.getString()));
            }
        } else if (right.getType() == ExpSource.Type.BIN_OPERAND) {
            binName = right.getBinName();
            if (left.getType() == ExpSource.Type.NUMBER_OPERAND) {
                exp = operator.apply(Exp.val(left.getNumber()), Exp.bin(binName, Exp.Type.INT));
            }
            if (left.getType() == ExpSource.Type.STRING_OPERAND) {
                exp = operator.apply(Exp.val(left.getString()), Exp.bin(binName, Exp.Type.STRING));
            }
        }

        return exp;
    }

    @Override
    public ExpSource visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        ExpSource left = visit(ctx.operand(0));
        ExpSource right = visit(ctx.operand(1));

        Exp exp = getExp(left, right, Exp::ge);
        return new Expr(exp);
    }

    @Override
    public ExpSource visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        ExpSource left = visit(ctx.operand(0));
        ExpSource right = visit(ctx.operand(1));

        Exp exp = getExp(left, right, Exp::lt);
        return new Expr(exp);
    }

    @Override
    public ExpSource visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        ExpSource left = visit(ctx.operand(0));
        ExpSource right = visit(ctx.operand(1));

        Exp exp = getExp(left, right, Exp::le);
        return new Expr(exp);
    }

    @Override
    public ExpSource visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        ExpSource left = visit(ctx.operand(0));
        ExpSource right = visit(ctx.operand(1));

        Exp exp = getExp(left, right, Exp::eq);
        return new Expr(exp);
    }

    @Override
    public ExpSource visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        ExpSource left = visit(ctx.operand(0));
        ExpSource right = visit(ctx.operand(1));

        Exp exp = getExp(left, right, Exp::ne);
        return new Expr(exp);
    }

    @Override
    public ExpSource visitPathFunction(ConditionParser.PathFunctionContext ctx) {
        String functionName = ctx.getText();
        return visitPathFunctionName(functionName);
    }

    private ExpSource visitPathFunctionName(String functionName) {
        Exp exp = switch (functionName) {
            case "exists" -> Exp.binExists("test"); // TODO: get the preceding path
            default -> throw new IllegalArgumentException("Unknown path function: " + functionName);
        };

        return new Expr(exp);
    }

    @Override
    public ExpSource visitMetadata(ConditionParser.MetadataContext ctx) {
        String functionName;
        Integer optionalParam = null;
        if (ctx.metadataFunction() == null) {
            functionName = ctx.digestModulo().getText();
            optionalParam = Integer.valueOf(ctx.NUMBER().getText());
        } else {
            functionName = ctx.metadataFunction().getText();
        }
        return visitMetadataFunctionName(functionName, optionalParam);
    }

    @Override
    public ExpSource visitMetadataFunction(ConditionParser.MetadataFunctionContext ctx) {
        String functionName = ctx.getText();
        return visitMetadataFunctionName(functionName, null);
    }

    private ExpSource visitMetadataFunctionName(String functionName, Integer optionalParam) {
        Exp exp = switch (functionName) {
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
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };

        return new Expr(exp);
    }

    @Override
    public ExpSource visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }

    @Override
    public ExpSource visitPathPart(ConditionParser.PathPartContext ctx) {
        return new BinOperand(ctx.NAME_IDENTIFIER().getText());
    }

    @Override
    public ExpSource visitQuotedString(ConditionParser.QuotedStringContext ctx) {
        String text = getWithoutQuotes(ctx.getText());
        return new StringOperand(text);
    }

    @Override
    public ExpSource visitNumber(ConditionParser.NumberContext ctx) {
        String text = ctx.getText();
        return new NumberOperand(Long.parseLong(text));
    }

    @Override
    protected ExpSource aggregateResult(ExpSource aggregate, ExpSource nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
