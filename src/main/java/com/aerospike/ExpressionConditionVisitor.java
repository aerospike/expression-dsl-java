package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.expSource.BinOperand;
import com.aerospike.expSource.AbstractPart;
import com.aerospike.expSource.Expr;
import com.aerospike.expSource.MetadataOperand;
import com.aerospike.expSource.NumberOperand;
import com.aerospike.expSource.StringOperand;

import java.util.function.BinaryOperator;

import static com.aerospike.util.ParsingUtils.getWithoutQuotes;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<AbstractPart> {

    @Override
    public AbstractPart visitAndExpression(ConditionParser.AndExpressionContext ctx) {
        Expr left = (Expr) visit(ctx.expression(0));
        Expr right = (Expr) visit(ctx.expression(1));

        return new Expr(Exp.and(left.getExp(), right.getExp()));
    }

    @Override
    public AbstractPart visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        Expr left = (Expr) visit(ctx.expression(0));
        Expr right = (Expr) visit(ctx.expression(1));

        return new Expr(Exp.or(left.getExp(), right.getExp()));
    }

    @Override
    public AbstractPart visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        Exp exp = visit(ctx.expression()).getExp();

        return new Expr(Exp.not(exp));
    }

    @Override
    public AbstractPart visitExclusiveExpression(ConditionParser.ExclusiveExpressionContext ctx) {
        Expr left = (Expr) visit(ctx.expression(0));
        Expr right = (Expr) visit(ctx.expression(1));

        return new Expr(Exp.exclusive(left.getExp(), right.getExp()));
    }

    @Override
    public AbstractPart visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::gt);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::ge);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::lt);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::le);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::eq);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::ne);
        return new Expr(exp);
    }

    private Exp getExpOrFail(AbstractPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        String binName;
        Exp exp = null;

        if (left == null) {
            throw new IllegalArgumentException("Unable to parse left operand");
        }
        if (right == null) {
            throw new IllegalArgumentException("Unable to parse right operand");
        }

        if (left.getType() == AbstractPart.Type.BIN_OPERAND) {
            binName = ((BinOperand) left).getBinName();
            exp = switch (right.getType()) {
                case NUMBER_OPERAND -> operator.apply(Exp.bin(binName, Exp.Type.INT), Exp.val(((NumberOperand) right).getNumber()));
                case STRING_OPERAND -> operator.apply(Exp.bin(binName, Exp.Type.STRING), Exp.val(((StringOperand) right).getString()));
                case METADATA_OPERAND -> operator.apply(
                        Exp.bin(binName, Exp.Type.valueOf(((MetadataOperand) right).getMetadataType().toString())),
                        right.getExp()
                );
                case EXPR -> operator.apply(Exp.bin(binName, Exp.Type.STRING), right.getExp());
                default -> exp;
            };
            return exp;
        }
        if (right.getType() == AbstractPart.Type.BIN_OPERAND) {
            binName = ((BinOperand) right).getBinName();
            exp = switch (left.getType()) {
                case NUMBER_OPERAND -> operator.apply(Exp.val(((NumberOperand) left).getNumber()), Exp.bin(binName, Exp.Type.INT));
                case STRING_OPERAND -> operator.apply(Exp.val(((StringOperand) left).getString()), Exp.bin(binName, Exp.Type.STRING));
                case METADATA_OPERAND -> operator.apply(
                        left.getExp(),
                        Exp.bin(binName, Exp.Type.valueOf(((MetadataOperand) left).getMetadataType().toString()))
                );
                case EXPR -> operator.apply(left.getExp(), Exp.bin(binName, Exp.Type.STRING));
                default -> exp;
            };
            return exp;
        }

        Exp leftExp = getExpForNonBinOperand(left);
        Exp rightExp = getExpForNonBinOperand(right);
        return operator.apply(leftExp, rightExp);
    }

    private Exp getExpForNonBinOperand(AbstractPart expSource) {
        return switch (expSource.getType()) {
            case NUMBER_OPERAND -> Exp.val(((NumberOperand) expSource).getNumber());
            case STRING_OPERAND -> Exp.val(((StringOperand) expSource).getString());
            case EXPR, METADATA_OPERAND -> expSource.getExp();
            default -> throw new IllegalStateException("Error: expecting non-bin operand, got " + expSource.getType());
        };
    }

    @Override
    public AbstractPart visitPathFunction(ConditionParser.PathFunctionContext ctx) {
        String functionName = ctx.getText();
        return visitPathFunctionName(functionName);
    }

    private AbstractPart visitPathFunctionName(String functionName) {
        Exp exp = switch (functionName) {
            case "exists" -> Exp.binExists("test"); // TODO: get the preceding path
            default -> throw new IllegalArgumentException("Unknown path function: " + functionName);
        };

        return new Expr(exp);
    }

    @Override
    public AbstractPart visitMetadata(ConditionParser.MetadataContext ctx) {
        String functionName;
        Integer optionalParam = null;
        if (ctx.metadataFunction() == null) {
            functionName = ctx.DIGEST_MODULO().getText();
            optionalParam = Integer.valueOf(ctx.NUMBER().getText());
        } else {
            functionName = ctx.metadataFunction().getText();
            functionName = functionName.substring(0, functionName.length() - 2); // remove parentheses
        }
        return visitMetadataFunctionName(functionName, optionalParam);
    }

    @Override
    public AbstractPart visitMetadataFunction(ConditionParser.MetadataFunctionContext ctx) {
        String functionName = ctx.getText();
        functionName = functionName.substring(0, functionName.length() - 2); // remove parentheses
        return visitMetadataFunctionName(functionName, null);
    }

    private AbstractPart visitMetadataFunctionName(String functionName, Integer optionalParam) {
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

        return new MetadataOperand(exp, functionName);
    }

    @Override
    public AbstractPart visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }

    @Override
    public AbstractPart visitPathPart(ConditionParser.PathPartContext ctx) {
        return new BinOperand(ctx.NAME_IDENTIFIER().getText());
    }

    @Override
    public AbstractPart visitQuotedString(ConditionParser.QuotedStringContext ctx) {
        String text = getWithoutQuotes(ctx.getText());
        return new StringOperand(text);
    }

    @Override
    public AbstractPart visitNumber(ConditionParser.NumberContext ctx) {
        String text = ctx.getText();
        return new NumberOperand(Long.parseLong(text));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
