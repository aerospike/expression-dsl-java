package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.parts.*;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;
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

        if (left.getType() == AbstractPart.Type.BIN_PART) {
            binName = ((BinPart) left).getBinName();
            exp = switch (right.getType()) {
                case NUMBER_OPERAND ->
                        operator.apply(Exp.bin(binName, Exp.Type.INT), Exp.val(((NumberOperand) right).getNumber()));
                case STRING_OPERAND ->
                        operator.apply(Exp.bin(binName, Exp.Type.STRING), Exp.val(((StringOperand) right).getString()));
                case METADATA_OPERAND -> operator.apply(
                        Exp.bin(binName, Exp.Type.valueOf(((MetadataOperand) right).getMetadataType().toString())),
                        right.getExp()
                );
                case EXPR -> operator.apply(Exp.bin(binName, Exp.Type.STRING), right.getExp());
                case PATH_OPERAND ->
                        operator.apply(Exp.bin(binName, Exp.Type.STRING), right.getExp()); // TODO: bin type
                default -> exp;
            };
            return exp;
        }
        if (right.getType() == AbstractPart.Type.BIN_PART) {
            binName = ((BinPart) right).getBinName();
            exp = switch (left.getType()) {
                case NUMBER_OPERAND ->
                        operator.apply(Exp.val(((NumberOperand) left).getNumber()), Exp.bin(binName, Exp.Type.INT));
                case STRING_OPERAND ->
                        operator.apply(Exp.val(((StringOperand) left).getString()), Exp.bin(binName, Exp.Type.STRING));
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

    private Exp getExpForNonBinOperand(AbstractPart part) {
        return switch (part.getType()) {
            case NUMBER_OPERAND -> Exp.val(((NumberOperand) part).getNumber());
            case STRING_OPERAND -> Exp.val(((StringOperand) part).getString());
            case EXPR, METADATA_OPERAND, PATH_OPERAND -> part.getExp();
            default -> throw new IllegalStateException("Error: expecting non-bin operand, got " + part.getType());
        };
    }

    @Override
    public AbstractPart visitPathFunctionSize(ConditionParser.PathFunctionSizeContext ctx) {
        return new PathFunction(PathFunction.PATH_FUNCTION_TYPE.SIZE, null, null);
    }

    @Override
    public AbstractPart visitPathFunctionGet(ConditionParser.PathFunctionGetContext ctx) {
        PathFunction.RETURN_PARAM returnParam = null;
        PathFunction.TYPE_PARAM typeParam = null;
        for (ConditionParser.PathFunctionParamContext paramCtx : ctx.pathFunctionParams().pathFunctionParam()) {
            if (paramCtx != null) {
                String typeVal = getPathFunctionParam(paramCtx, "type");
                if (typeVal != null) typeParam = PathFunction.TYPE_PARAM.valueOf(typeVal);
                String returnVal = getPathFunctionParam(paramCtx, "return");
                if (returnVal != null) returnParam = PathFunction.RETURN_PARAM.valueOf(returnVal);
            }
        }
        return new PathFunction(PathFunction.PATH_FUNCTION_TYPE.GET, returnParam, typeParam);
    }

    private String getPathFunctionParam(ConditionParser.PathFunctionParamContext paramCtx, String paramName) {
        String paramNameText;
        String paramNameValue;
        String paramValue = null;
        if (paramCtx.pathFunctionParamName() != null) {
            paramNameText = paramCtx.pathFunctionParamName().getText();
            paramNameValue = paramCtx.pathFunctionParamValue().getText();
            if (paramNameText.equalsIgnoreCase(paramName)) {
                paramValue = paramNameValue;
            }
        }
        return paramValue;
    }

    @Override
    public AbstractPart visitMetadata(ConditionParser.MetadataContext ctx) {
        String functionName;
        Integer optionalParam = null;
        if (ctx.METADATA_FUNCTION() == null) {
            functionName = ctx.digestModulo().DIGEST_MODULO().getText();
            optionalParam = Integer.valueOf(ctx.digestModulo().NUMBER().getText());
        } else {
            functionName = ctx.METADATA_FUNCTION().getText();
            functionName = functionName.substring(0, functionName.length() - 2); // remove parentheses
        }
        return visitMetadataFunctionName(functionName, optionalParam);
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
        return new BinPart(ctx.NAME_IDENTIFIER().getText());
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
    public AbstractPart visitBasePath(ConditionParser.BasePathContext ctx) {
        BinPart binOperand = null;
        List<AbstractPart> parts = new ArrayList<>();
        List<ParseTree> ctxChildrenExclDots = ctx.children.stream()
                .filter(tree -> !tree.getText().equals("."))
                .toList();

        for (ParseTree child : ctxChildrenExclDots) {
            AbstractPart part = visit(child);
            switch (part.getType()) {
                case BIN_PART -> {
                    binOperand = (BinPart) part;
                }
                case LIST_PART -> {
                    parts.add(part);
                }
                default -> throw new IllegalStateException("Unexpected path part: " + part.getType());
            }
        }

        if (binOperand == null) {
            throw new IllegalArgumentException("Expecting bin to be the first path part from the left");
        }

        return new BasePath(binOperand, parts);
    }

    @Override
    public AbstractPart visitPath(ConditionParser.PathContext ctx) {
        BasePath basePath = (BasePath) visit(ctx.basePath());
        Exp exp = PathOperand.processPath(basePath, ctx.pathFunction() == null ? null : (PathFunction) visit(ctx.pathFunction()));
        return new PathOperand(exp);
    }

    @Override
    public AbstractPart visitListPath(ConditionParser.ListPathContext ctx) {
        if (ctx.LIST_BIN() != null) return ListPart.builder()
                .setListBin(true)
                .build();

        if (ctx.listIndex() != null) return ListPart.builder()
                .setListIndex(Integer.parseInt(ctx.listIndex().NUMBER().getText()))
                .build();

        if (ctx.listValue() != null) return ListPart.builder()
                .setListValue(ctx.listValue().NAME_IDENTIFIER().getText())
                .build();

        if (ctx.listRank() != null) return ListPart.builder()
                .setListRank(Integer.parseInt(ctx.listRank().NUMBER().getText()))
                .build();

        throw new IllegalStateException("Unexpected path type in a List: " + ctx.getText());
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
