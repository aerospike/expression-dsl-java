package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.model.AbstractPart;
import com.aerospike.model.BinOperand;
import com.aerospike.model.Expr;
import com.aerospike.model.FloatOperand;
import com.aerospike.model.IntOperand;
import com.aerospike.model.MetadataOperand;
import com.aerospike.model.StringOperand;

import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

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

    @Override
    public AbstractPart visitAddExpression(ConditionParser.AddExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::add);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitSubExpression(ConditionParser.SubExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::sub);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitMulExpression(ConditionParser.MulExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::mul);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitDivExpression(ConditionParser.DivExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::div);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitModExpression(ConditionParser.ModExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::mod);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitIntAndExpression(ConditionParser.IntAndExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::intAnd);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitIntOrExpression(ConditionParser.IntOrExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::intOr);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitIntXorExpression(ConditionParser.IntXorExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::intXor);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitIntNotExpression(ConditionParser.IntNotExpressionContext ctx) {
        AbstractPart operand = visit(ctx.operand());

        Exp exp = getExpOrFail(operand, Exp::intNot);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitIntLShiftExpression(ConditionParser.IntLShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::lshift);
        return new Expr(exp);
    }

    @Override
    public AbstractPart visitIntRShiftExpression(ConditionParser.IntRShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Exp exp = getExpOrFail(left, right, Exp::rshift);
        return new Expr(exp);
    }

    private Exp getExpOrFail(AbstractPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        String binNameRight;
        Exp exp = null;

        if (left == null) {
            throw new IllegalArgumentException("Unable to parse left operand");
        }
        if (right == null) {
            throw new IllegalArgumentException("Unable to parse right operand");
        }

        if (left.getType() == AbstractPart.Type.BIN_OPERAND) {
            String binNameLeft = ((BinOperand) left).getBinName();
            exp = switch (right.getType()) {
                case INT_OPERAND ->
                        operator.apply(Exp.bin(binNameLeft, Exp.Type.INT), Exp.val(((IntOperand) right).getValue()));
                case FLOAT_OPERAND ->
                        operator.apply(Exp.bin(binNameLeft, Exp.Type.FLOAT), Exp.val(((FloatOperand) right).getValue()));
                case STRING_OPERAND ->
                        operator.apply(Exp.bin(binNameLeft, Exp.Type.STRING), Exp.val(((StringOperand) right).getString()));
                case METADATA_OPERAND -> operator.apply(
                        Exp.bin(binNameLeft, Exp.Type.valueOf(((MetadataOperand) right).getMetadataType().toString())),
                        right.getExp()
                );
                case EXPR -> operator.apply(Exp.bin(binNameLeft, Exp.Type.STRING), right.getExp());
                // By default, compare bins as integers unless provided an explicit type to compare
                case BIN_OPERAND -> {
                    binNameRight = ((BinOperand) right).getBinName();
                    yield operator.apply(Exp.bin(binNameLeft, Exp.Type.INT), Exp.bin(binNameRight, Exp.Type.INT));
                }
            };
            return exp;
        }
        if (right.getType() == AbstractPart.Type.BIN_OPERAND) {
            binNameRight = ((BinOperand) right).getBinName();
            exp = switch (left.getType()) {
                case INT_OPERAND ->
                        operator.apply(Exp.val(((IntOperand) left).getValue()), Exp.bin(binNameRight, Exp.Type.INT));
                case FLOAT_OPERAND ->
                        operator.apply(Exp.val(((FloatOperand) left).getValue()), Exp.bin(binNameRight, Exp.Type.FLOAT));
                case STRING_OPERAND ->
                        operator.apply(Exp.val(((StringOperand) left).getString()), Exp.bin(binNameRight, Exp.Type.STRING));
                case METADATA_OPERAND -> operator.apply(
                        left.getExp(),
                        Exp.bin(binNameRight, Exp.Type.valueOf(((MetadataOperand) left).getMetadataType().toString()))
                );
                case EXPR -> operator.apply(left.getExp(), Exp.bin(binNameRight, Exp.Type.STRING));
                // No need for 2 BIN_OPERAND handling since it's covered in the left condition
                default -> exp;
            };
            return exp;
        }

        // Handle non Bin operands cases
        Exp leftExp = getExpForNonBinOperand(left);
        Exp rightExp = getExpForNonBinOperand(right);
        return operator.apply(leftExp, rightExp);
    }

    /*
        For 1 operand Expressions
     */
    private Exp getExpOrFail(AbstractPart operand, UnaryOperator<Exp> operator) {
        if (operand == null) {
            throw new IllegalArgumentException("Unable to parse operand");
        }

        // 1 Operand Expression is always a BIN Operand
        String binName = ((BinOperand) operand).getBinName();

        return operator.apply(Exp.bin(binName, Exp.Type.INT));
    }

    private Exp getExpForNonBinOperand(AbstractPart expSource) {
        return switch (expSource.getType()) {
            case INT_OPERAND -> Exp.val(((IntOperand) expSource).getValue());
            case FLOAT_OPERAND -> Exp.val(((FloatOperand) expSource).getValue());
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
        String text = ctx.METADATA_FUNCTION().getText();
        String functionName = extractFunctionName(text);
        Integer parameter = extractParameter(text);

        if (parameter != null) {
            return new MetadataOperand(functionName, parameter);
        } else {
            return new MetadataOperand(functionName);
        }
    }

    private String extractFunctionName(String text) {
        int startParen = text.indexOf('(');
        return (startParen != -1) ? text.substring(0, startParen) : text;
    }

    private Integer extractParameter(String text) {
        int startParen = text.indexOf('(');
        int endParen = text.indexOf(')');

        if (startParen != -1 && endParen != -1 && endParen > startParen + 1) {
            String numberStr = text.substring(startParen + 1, endParen);
            return Integer.parseInt(numberStr);
        }
        return null;
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
    public AbstractPart visitNumberOperand(ConditionParser.NumberOperandContext ctx) {
        // Delegates to specific visit methods
        return visitChildren(ctx);
    }

    @Override
    public AbstractPart visitIntOperand(ConditionParser.IntOperandContext ctx) {
        String text = ctx.INT().getText();
        return new IntOperand(Long.parseLong(text));
    }

    @Override
    public AbstractPart visitFloatOperand(ConditionParser.FloatOperandContext ctx) {
        String text = ctx.FLOAT().getText();
        return new FloatOperand(Double.parseDouble(text));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
