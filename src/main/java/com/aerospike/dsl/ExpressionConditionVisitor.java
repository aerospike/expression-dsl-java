package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.*;
import com.aerospike.dsl.util.ValidationUtils;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import static com.aerospike.dsl.util.ParsingUtils.getWithoutQuotes;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<AbstractPart> {

    @Override
    public AbstractPart visitWhenExpression(ConditionParser.WhenExpressionContext ctx) {
        List<Exp> expressions = new ArrayList<>();

        // for each condition declaration
        for (ConditionParser.ExpressionMappingContext emc : ctx.expressionMapping()) {
            // visit condition
            expressions.add(visit(emc.expression(0)).getExp());
            // visit action
            expressions.add(visit(emc.expression(1)).getExp());
        }

        // visit default
        expressions.add(visit(ctx.expression()).getExp());
        return new Expr(Exp.cond(expressions.toArray(new Exp[0])));
    }

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

    // 2 operands Expressions
    private Exp getExpOrFail(AbstractPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        if (left == null) {
            throw new AerospikeDSLException("Unable to parse left operand");
        }
        if (right == null) {
            throw new AerospikeDSLException("Unable to parse right operand");
        }

        if (left.getPartType() == AbstractPart.PartType.BIN_PART) {
            return getExpLeftBinTypeComparison((BinPart) left, right, operator);
        }
        if (right.getPartType() == AbstractPart.PartType.BIN_PART) {
            return getExpRightBinTypeComparison(left, (BinPart) right, operator);
        }

        // Handle non Bin operands cases
        Exp leftExp = getExpForNonBinOperand(left);
        Exp rightExp = getExpForNonBinOperand(right);
        return operator.apply(leftExp, rightExp);
    }

    private Exp getExpLeftBinTypeComparison(BinPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        String binNameLeft = left.getBinName();
        return switch (right.getPartType()) {
            case INT_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.INT);
                yield operator.apply(Exp.bin(binNameLeft, Exp.Type.INT), Exp.val(((IntOperand) right).getValue()));
            }
            case FLOAT_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.FLOAT);
                yield operator.apply(Exp.bin(binNameLeft, Exp.Type.FLOAT), Exp.val(((FloatOperand) right).getValue()));
            }
            case BOOL_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.BOOL);
                yield operator.apply(Exp.bin(binNameLeft, Exp.Type.BOOL), Exp.val(((BooleanOperand) right).getValue()));
            }
            case STRING_OPERAND -> {
                if (left.getExpType() != null &&
                        left.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.BLOB);
                    String base64String = ((StringOperand) right).getString();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield operator.apply(Exp.bin(binNameLeft, Exp.Type.BLOB), Exp.val(value));
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.STRING);
                    yield operator.apply(Exp.bin(binNameLeft, Exp.Type.STRING), Exp.val(((StringOperand) right).getString()));
                }
            }
            case METADATA_OPERAND -> {
                // No need to validate, types are determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) right).getMetadataType().toString());
                yield operator.apply(
                        Exp.bin(binNameLeft, binType),
                        right.getExp()
                );
            }
            case EXPR -> {
                // Can't validate with Expr on one side
                Exp.Type leftExplicitType = left.getExpType();
                if (leftExplicitType != null) {
                    yield operator.apply(Exp.bin(binNameLeft, leftExplicitType), right.getExp());
                } else {
                    yield operator.apply(Exp.bin(binNameLeft, Exp.Type.INT), right.getExp());
                }
            }
            case PATH_OPERAND -> {
                Exp.Type leftExplicitType = left.getExpType();
                // Can't validate with Path on one side
                if (leftExplicitType != null) {
                    yield operator.apply(Exp.bin(binNameLeft, leftExplicitType), right.getExp());
                } else {
                    yield operator.apply(Exp.bin(binNameLeft, Exp.Type.STRING), right.getExp());
                }
            }
            // By default, compare bins as integers unless provided an explicit type to compare
            case BIN_PART -> {
                String binNameRight = ((BinPart) right).getBinName();
                Exp.Type leftExplicitType = left.getExpType();
                Exp.Type rightExplicitType = ((BinPart) right).getExpType();

                if (leftExplicitType != null && rightExplicitType != null) {
                    ValidationUtils.validateComparableTypes(leftExplicitType, rightExplicitType);
                    yield operator.apply(
                            Exp.bin(binNameLeft, left.getExpType()),
                            Exp.bin(binNameRight, ((BinPart) right).getExpType()));
                } else {
                    yield operator.apply(
                            Exp.bin(binNameLeft, Exp.Type.INT),
                            Exp.bin(binNameRight, Exp.Type.INT));
                }
            }
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(right.getPartType()));
        };
    }

    private Exp getExpRightBinTypeComparison(AbstractPart left, BinPart right, BinaryOperator<Exp> operator) {
        String binNameRight = right.getBinName();
        return switch (left.getPartType()) {
            case INT_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.INT, right.getExpType());
                yield operator.apply(Exp.val(((IntOperand) left).getValue()), Exp.bin(binNameRight, Exp.Type.INT));
            }
            case FLOAT_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.FLOAT, right.getExpType());
                yield operator.apply(Exp.val(((FloatOperand) left).getValue()), Exp.bin(binNameRight, Exp.Type.FLOAT));
            }
            case BOOL_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.BOOL, right.getExpType());
                yield operator.apply(Exp.val(((BooleanOperand) left).getValue()), Exp.bin(binNameRight, Exp.Type.BOOL));
            }
            case STRING_OPERAND -> {
                if (right.getExpType() != null &&
                        right.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(Exp.Type.BLOB, right.getExpType());
                    String base64String = ((StringOperand) left).getString();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield operator.apply(Exp.val(value), Exp.bin(binNameRight, Exp.Type.BLOB));
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(Exp.Type.STRING, right.getExpType());
                    yield operator.apply(Exp.val(((StringOperand) left).getString()), Exp.bin(binNameRight, Exp.Type.STRING));
                }
            }
            case METADATA_OPERAND -> {
                // No need to validate, types are determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) left).getMetadataType().toString());
                yield operator.apply(
                        left.getExp(),
                        Exp.bin(binNameRight, binType)
                );
            }
            case EXPR -> {
                // Can't validate with Expr on one side
                Exp.Type rightExplicitType = right.getExpType();
                if (rightExplicitType != null) {
                    yield operator.apply(left.getExp(), Exp.bin(binNameRight, rightExplicitType));
                } else {
                    yield operator.apply(left.getExp(), Exp.bin(binNameRight, Exp.Type.INT));
                }
            }
            case PATH_OPERAND -> {
                // Can't validate with Path on one side
                Exp.Type rightExplicitType = right.getExpType();
                if (rightExplicitType != null) {
                    yield operator.apply(left.getExp(), Exp.bin(binNameRight, rightExplicitType));
                } else {
                    yield operator.apply(left.getExp(), Exp.bin(binNameRight, Exp.Type.STRING));
                }
            }
            // No need for 2 BIN_OPERAND handling since it's covered in the left condition
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(left.getPartType()));
        };
    }

    // 1 operand Expressions
    private Exp getExpOrFail(AbstractPart operand, UnaryOperator<Exp> operator) {
        if (operand == null) {
            throw new AerospikeDSLException("Unable to parse operand");
        }

        // 1 Operand Expression is always a BIN Operand
        String binName = ((BinPart) operand).getBinName();

        return operator.apply(Exp.bin(binName, Exp.Type.INT));
    }

    private Exp getExpForNonBinOperand(AbstractPart part) {
        return switch (part.getPartType()) {
            case INT_OPERAND -> Exp.val(((IntOperand) part).getValue());
            case FLOAT_OPERAND -> Exp.val(((FloatOperand) part).getValue());
            case STRING_OPERAND -> Exp.val(((StringOperand) part).getString());
            case BOOL_OPERAND -> Exp.val(((BooleanOperand) part).getValue());
            case EXPR, METADATA_OPERAND, PATH_OPERAND -> part.getExp();
            default -> throw new AerospikeDSLException("Expecting non-bin operand, got " + part.getPartType());
        };
    }

    @Override
    public AbstractPart visitPathFunctionSize(ConditionParser.PathFunctionSizeContext ctx) {
        return new PathFunction(PathFunction.PathFunctionType.SIZE, null, null);
    }

    @Override
    public AbstractPart visitPathFunctionGet(ConditionParser.PathFunctionGetContext ctx) {
        PathFunction.ReturnParam returnParam = null;
        Exp.Type binType = null;
        for (ConditionParser.PathFunctionParamContext paramCtx : ctx.pathFunctionParams().pathFunctionParam()) {
            if (paramCtx != null) {
                String typeVal = getPathFunctionParam(paramCtx, "type");
                if (typeVal != null) binType = Exp.Type.valueOf(typeVal);
                String returnVal = getPathFunctionParam(paramCtx, "return");
                if (returnVal != null) returnParam = PathFunction.ReturnParam.valueOf(returnVal);
            }
        }
        return new PathFunction(PathFunction.PathFunctionType.GET, returnParam, binType);
    }

    @Override
    public AbstractPart visitPathFunctionCount(ConditionParser.PathFunctionCountContext ctx) {
        return new PathFunction(PathFunction.PathFunctionType.COUNT, PathFunction.ReturnParam.COUNT, null); // todo: TYPE_PARAM?
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
    public AbstractPart visitPathFunctionCast(ConditionParser.PathFunctionCastContext ctx) {
        String typeVal = extractTypeFromMethod(ctx.PATH_FUNCTION_CAST().getText());
        PathFunction.CastType castType = PathFunction.CastType.valueOf(typeVal.toUpperCase());
        Exp.Type binType = PathFunction.castTypeToExpType(castType);

        return new PathFunction(PathFunction.PathFunctionType.CAST, null, binType);
    }

    private static String extractTypeFromMethod(String methodName) {
        if (methodName.startsWith("as") && methodName.endsWith("()")) {
            return methodName.substring(2, methodName.length() - 2);
        } else {
            throw new AerospikeDSLException("Invalid method name: %s".formatted(methodName));
        }
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
        return new BinPart(ctx.NAME_IDENTIFIER().getText());
    }

    @Override
    public AbstractPart visitStringOperand(ConditionParser.StringOperandContext ctx) {
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
    public AbstractPart visitBooleanOperand(ConditionParser.BooleanOperandContext ctx) {
        String text = ctx.getText();
        return new BooleanOperand(Boolean.parseBoolean(text));
    }

    @Override
    public AbstractPart visitBasePath(ConditionParser.BasePathContext ctx) {
        BinPart binPart = null;
        List<AbstractPart> parts = new ArrayList<>();
        List<ParseTree> ctxChildrenExclDots = ctx.children.stream()
                .filter(tree -> !tree.getText().equals("."))
                .toList();

        for (ParseTree child : ctxChildrenExclDots) {
            AbstractPart part = visit(child);
            switch (part.getPartType()) {
                case BIN_PART -> binPart = overrideBinType(part, ctx);
                case LIST_PART -> parts.add(part);
                default -> throw new AerospikeDSLException("Unexpected path part: %s".formatted(part.getPartType()));
            }
        }

        if (binPart == null) {
            throw new AerospikeDSLException("Expecting bin to be the first path part from the left");
        }

        return new BasePath(binPart, parts);
    }

    private BinPart overrideBinType(AbstractPart part, ConditionParser.BasePathContext ctx) {
        BinPart binPart = (BinPart) part;

        ConditionParser.PathFunctionContext pathFunctionContext = ((ConditionParser.PathContext) ctx.getParent()).pathFunction();

        // In case we have a path function (explicit get or cast) override the type
        if (pathFunctionContext != null) {
            PathFunction pathFunction = (PathFunction) visit(pathFunctionContext);

            if (pathFunction != null) {
                Exp.Type type = pathFunction.getBinType();
                if (type != null) {
                    binPart.setExpType(type);
                }
            }
        } else { // Implicit detect for Float type
            if (implicitDetectFloatFromUpperTree(ctx)) {
                binPart.setExpType(Exp.Type.FLOAT);
            }
        }
        return binPart;
    }

    /*
        Return true if implicitly required to compare current expression branch as floats.
        Implicit casting of a complicated expression should only detect floats:
        1. Arithmetic expressions only works on numbers, so by default int unless we detect a float operand
        2. Logical expressions always operate on booleans
        3. Metadata expressions we know the type to compare, and it's never a float
        4. Comparison expressions can be everything, but they do not alter and always return a boolean
     */
    private boolean implicitDetectFloatFromUpperTree(ConditionParser.BasePathContext ctx) {
        ParserRuleContext obj = ctx;

        // Search for a float operand child in the above levels of the current path in the expression tree
        while (obj.getParent() != null) {
            obj = obj.getParent();

            for (ParseTree child : obj.children) {
                if (child instanceof ConditionParser.OperandContext operandContext &&
                        operandContext.numberOperand() != null &&
                        operandContext.numberOperand().floatOperand() != null) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public AbstractPart visitPath(ConditionParser.PathContext ctx) {
        BasePath basePath = (BasePath) visit(ctx.basePath());
        List<AbstractPart> parts = basePath.getParts();

        // if there are other parts except bin, get a corresponding Exp
        if (!parts.isEmpty()) {
            Exp exp = PathOperand.processPath(basePath, ctx.pathFunction() == null
                    ? null
                    : (PathFunction) visit(ctx.pathFunction()));
            return new PathOperand(exp);
        }
        return basePath.getBinPart();
    }

    @Override
    public AbstractPart visitListPath(ConditionParser.ListPathContext ctx) {
        if (ctx.LIST_BIN() != null) return ListPart.builder()
                .setListBin(true)
                .build();

        if (ctx.listIndex() != null) return ListPart.builder()
                .setListIndex(Integer.parseInt(ctx.listIndex().INT().getText()))
                .build();

        if (ctx.listValue() != null) {
            String listValue = ctx.listValue().VALUE_IDENTIFIER().getText();
            return ListPart.builder()
                    .setListValue(listValue.substring(1))
                    .build();
        }

        if (ctx.listRank() != null) {
            String listRank = ctx.listRank().RANK_IDENTIFIER().getText();
            return ListPart.builder()
                    .setListRank(Integer.parseInt(listRank.substring(1)))
                    .build();
        }

        throw new AerospikeDSLException("Unexpected path type in a List: %s".formatted(ctx.getText()));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
