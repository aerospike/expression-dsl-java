package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.*;
import com.aerospike.dsl.util.ParsingUtils;
import com.aerospike.dsl.util.ValidationUtils;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import static com.aerospike.dsl.util.ParsingUtils.getWithoutQuotes;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<AbstractPart> {

    @Override
    public AbstractPart visitWithExpression(ConditionParser.WithExpressionContext ctx) {
        List<Exp> expressions = new ArrayList<>();

        // iterate each definition
        for (ConditionParser.VariableDefinitionContext vdc : ctx.variableDefinition()) {
            expressions.add(Exp.def(vdc.stringOperand().getText(), visit(vdc.expression()).getExp()));
        }
        // last expression is the action (described after "do")
        expressions.add(visit(ctx.expression()).getExp());
        return new Expr(Exp.let(expressions.toArray(new Exp[0])));
    }

    @Override
    public AbstractPart visitWhenExpression(ConditionParser.WhenExpressionContext ctx) {
        List<Exp> expressions = new ArrayList<>();

        // iterate each condition declaration
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

        logicalSetBinsAsBooleanExpr(left, right);
        return new Expr(Exp.and(left.getExp(), right.getExp()));
    }

    @Override
    public AbstractPart visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        Expr left = (Expr) visit(ctx.expression(0));
        Expr right = (Expr) visit(ctx.expression(1));

        logicalSetBinsAsBooleanExpr(left, right);
        return new Expr(Exp.or(left.getExp(), right.getExp()));
    }

    @Override
    public AbstractPart visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        Expr expr = (Expr) visit(ctx.expression());

        logicalSetBinAsBooleanExpr(expr);
        return new Expr(Exp.not(expr.getExp()));
    }

    @Override
    public AbstractPart visitExclusiveExpression(ConditionParser.ExclusiveExpressionContext ctx) {
        if (ctx.expression().size() < 2) {
            throw new AerospikeDSLException("Exclusive logical operator requires 2 or more expressions");
        }
        List<Exp> expressions = new ArrayList<>();

        // iterate each condition declaration
        for (ConditionParser.ExpressionContext ec : ctx.expression()) {
            Expr expr = (Expr) visit(ec);
            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr.getExp());
        }
        return new Expr(Exp.exclusive(expressions.toArray(new Exp[0])));
    }

    private void logicalSetBinsAsBooleanExpr(Expr left, Expr right) {
        logicalSetBinAsBooleanExpr(left);
        logicalSetBinAsBooleanExpr(right);
    }

    private void logicalSetBinAsBooleanExpr(Expr expr) {
        if (expr instanceof BinPart) {
            ((BinPart) expr).updateExp(Exp.Type.BOOL);
        }
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
        Exp leftExp = left.getExp();
        Exp rightExp = right.getExp();
        return operator.apply(leftExp, rightExp);
    }

    private Exp getExpLeftBinTypeComparison(BinPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        String binNameLeft = left.getBinName();
        return switch (right.getPartType()) {
            case INT_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.INT);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case FLOAT_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.FLOAT);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case BOOL_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.BOOL);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case STRING_OPERAND -> {
                if (left.getExpType() != null &&
                        left.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.BLOB);
                    String base64String = ((StringOperand) right).getString();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield operator.apply(left.getExp(), Exp.val(value));
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.STRING);
                    yield operator.apply(left.getExp(), right.getExp());
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
            case EXPR, PATH_OPERAND ->
                    operator.apply(left.getExp(), right.getExp()); // Can't validate with Expr on one side
            // Left and right are both bin parts
            case BIN_PART -> {
                // Validate types if possible
                ValidationUtils.validateComparableTypes(left.getExpType(), right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(right.getPartType()));
        };
    }

    private Exp getExpRightBinTypeComparison(AbstractPart left, BinPart right, BinaryOperator<Exp> operator) {
        String binNameRight = right.getBinName();
        return switch (left.getPartType()) {
            case INT_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.INT, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case FLOAT_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.FLOAT, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case BOOL_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.BOOL, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case STRING_OPERAND -> {
                if (right.getExpType() != null &&
                        right.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(Exp.Type.BLOB, right.getExpType());
                    String base64String = ((StringOperand) left).getString();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield operator.apply(Exp.val(value), right.getExp());
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(Exp.Type.STRING, right.getExpType());
                    yield operator.apply(left.getExp(), right.getExp());
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
            case EXPR, PATH_OPERAND ->
                    operator.apply(left.getExp(), right.getExp()); // Can't validate with Expr on one side
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

        // There is only 1 case of a single operand expression (int not), and it always gets an integer
        return operator.apply(Exp.bin(binName, Exp.Type.INT));
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
    public AbstractPart visitBinPart(ConditionParser.BinPartContext ctx) {
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
                case BIN_PART -> binPart = (BinPart) overrideType(part, ctx);
                case LIST_PART, MAP_PART -> parts.add(overrideType(part, ctx));
                default -> throw new AerospikeDSLException("Unexpected path part: %s".formatted(part.getPartType()));
            }
        }

        if (binPart == null) {
            throw new AerospikeDSLException("Expecting bin to be the first path part from the left");
        }

        return new BasePath(binPart, parts);
    }

    @Override
    public AbstractPart visitVariable(ConditionParser.VariableContext ctx) {
        String text = ctx.VARIABLE_REFERENCE().getText();
        return new VariableOperand(extractVariableName(text));
    }

    private String extractVariableName(String variableReference) {
        if (variableReference.startsWith("${") && variableReference.endsWith("}")) {
            return variableReference.substring(2, variableReference.length() - 1);
        } else {
            throw new IllegalArgumentException("Input string is not in the correct format");
        }
    }

    private AbstractPart overrideType(AbstractPart part, ParseTree ctx) {
        ConditionParser.PathFunctionContext pathFunctionContext =
                ((ConditionParser.PathContext) ctx.getParent()).pathFunction();

        // Override with Path Function (explicit get or cast)
        if (pathFunctionContext != null) {
            PathFunction pathFunction = (PathFunction) visit(pathFunctionContext);

            if (pathFunction != null) {
                Exp.Type type = pathFunction.getBinType();
                if (type != null) {
                    if (part instanceof BinPart) {
                        ((BinPart) part).updateExp(type);
                    } else {
                        part.setExpType(type);
                    }
                }
            }
        } else { // Override using Implicit type detection
            Exp.Type implicitType = detectImplicitTypeFromUpperTree(ctx);
            if (part instanceof BinPart) {
                ((BinPart) part).updateExp(implicitType);
            } else { // ListPart or MapPart
                part.setExpType(implicitType);
            }
        }
        return part;
    }

    private Exp.Type detectImplicitTypeFromUpperTree(ParseTree ctx) {
        // Search for a "leaf" operand child (Int, Float, String and Boolean)
        // in the above levels of the current path in the expression tree
        while (ctx.getParent() != null) {
            ctx = ctx.getParent();

            for (int i = 0; i < ctx.getChildCount(); i++) {
                ParseTree child = ctx.getChild(i);

                if (child instanceof ConditionParser.OperandContext operandContext) {
                    if (operandContext.numberOperand() != null) {
                        if (operandContext.numberOperand().intOperand() != null) {
                            return Exp.Type.INT;
                        } else if (operandContext.numberOperand().floatOperand() != null) {
                            return Exp.Type.FLOAT;
                        }
                    } else if (operandContext.stringOperand() != null) {
                        return Exp.Type.STRING;
                    } else if (operandContext.booleanOperand() != null) {
                        return Exp.Type.BOOL;
                    }
                }
            }
        }
        // Default INT
        return Exp.Type.INT;
    }

    @Override
    public AbstractPart visitPath(ConditionParser.PathContext ctx) {
        BasePath basePath = (BasePath) visit(ctx.basePath());
        List<AbstractPart> parts = basePath.getParts();

        // if there are other parts except bin, get a corresponding Exp
        if (!parts.isEmpty() || ctx.pathFunction() != null && ctx.pathFunction().pathFunctionSize() != null) {
            Exp exp = PathOperand.processPath(basePath, ctx.pathFunction() == null
                    ? null
                    : (PathFunction) visit(ctx.pathFunction()));
            return new PathOperand(exp);
        }
        return basePath.getBinPart();
    }

    @Override
    public AbstractPart visitListPart(ConditionParser.ListPartContext ctx) {
        if (ctx.LIST_BIN() != null) {
            return ListPart.builder()
                    .setListBin(true)
                    .build();
        }

        if (ctx.listIndex() != null) {
            return ListPart.builder()
                    .setListIndex(Integer.parseInt(ctx.listIndex().INT().getText()))
                    .build();
        }

        if (ctx.listValue() != null) {
            Object listValue = null;
            if (ctx.listValue().valueIdentifier().NAME_IDENTIFIER() != null) {
                listValue = ctx.listValue().valueIdentifier().NAME_IDENTIFIER().getText();
            } else if (ctx.listValue().valueIdentifier().QUOTED_STRING() != null) {
                listValue = ParsingUtils.getWithoutQuotes(ctx.listValue().valueIdentifier().QUOTED_STRING().getText());
            } else if (ctx.listValue().valueIdentifier().INT() != null) {
                listValue = Integer.parseInt(ctx.listValue().valueIdentifier().INT().getText());
            }
            return ListPart.builder()
                    .setListValue(listValue)
                    .build();
        }

        if (ctx.listRank() != null) {
            String listRank = ctx.listRank().INT().getText();
            return ListPart.builder()
                    .setListRank(Integer.parseInt(listRank))
                    .build();
        }

        if (ctx.listIndexRange() != null) {
            ConditionParser.StandardListIndexRangeContext indexRange = ctx.listIndexRange().standardListIndexRange();
            ConditionParser.InvertedListIndexRangeContext invertedIndexRange = ctx.listIndexRange().invertedListIndexRange();

            if (indexRange != null || invertedIndexRange != null) {
                ConditionParser.IndexRangeIdentifierContext range =
                        indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
                boolean isInverted = indexRange == null;

                Integer start = Integer.parseInt(range.start().INT().getText());
                Integer count = null;
                if (range.count() != null) {
                    count = Integer.parseInt(range.count().INT().getText());
                }

                return ListPart.builder()
                        .setListIndexRange(isInverted, start, count)
                        .build();
            }
        }

        if (ctx.listValueList() != null) {
            ConditionParser.StandardListValueListContext valueList = ctx.listValueList().standardListValueList();
            ConditionParser.InvertedListValueListContext invertedValueList = ctx.listValueList().invertedListValueList();

            if (valueList != null || invertedValueList != null) {
                ConditionParser.ValueListIdentifierContext list =
                        valueList != null ? valueList.valueListIdentifier() : invertedValueList.valueListIdentifier();
                boolean isInverted = valueList == null;

                List<?> valueListObjects = list.valueIdentifier().stream().map(
                        listValue -> {
                            if (listValue.NAME_IDENTIFIER() != null) {
                                return listValue.NAME_IDENTIFIER().getText();
                            } else if (listValue.QUOTED_STRING() != null) {
                                return getWithoutQuotes(listValue.QUOTED_STRING().getText());
                            } else {
                                return Integer.parseInt(listValue.INT().getText());
                            }
                        }
                ).toList();

                return ListPart.builder()
                        .setListValueList(isInverted, valueListObjects)
                        .build();
            }
        }

        if (ctx.listValueRange() != null) {
            ConditionParser.StandardListValueRangeContext valueRange = ctx.listValueRange().standardListValueRange();
            ConditionParser.InvertedListValueRangeContext invertedValueRange = ctx.listValueRange().invertedListValueRange();

            if (valueRange != null || invertedValueRange != null) {
                ConditionParser.ValueRangeIdentifierContext range =
                        valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
                boolean isInverted = valueRange == null;

                Integer startValue = Integer.parseInt(range.valueIdentifier(0).INT().getText());

                Integer endValue = null;

                if (range.valueIdentifier(1) != null) {
                    if (range.valueIdentifier(1).INT() != null) {
                        endValue = Integer.parseInt(range.valueIdentifier(1).INT().getText());
                    }
                }

                return ListPart.builder()
                        .setListValueRange(isInverted, startValue, endValue)
                        .build();
            }
        }

        throw new AerospikeDSLException("Unexpected path type in a List: %s".formatted(ctx.getText()));
    }

    @Override
    public AbstractPart visitMapPart(ConditionParser.MapPartContext ctx) {
        if (ctx.mapKey() != null) {
            if (ctx.mapKey().QUOTED_STRING() != null) {
                return MapPart.builder()
                        .setMapKey(ParsingUtils.getWithoutQuotes(ctx.mapKey().QUOTED_STRING().getText()))
                        .build();
            }
            if (ctx.mapKey().NAME_IDENTIFIER() != null) {
                return MapPart.builder()
                        .setMapKey(ctx.mapKey().NAME_IDENTIFIER().getText())
                        .build();
            }
        }

        if (ctx.mapIndex() != null) {
            return MapPart.builder()
                    .setMapIndex(Integer.parseInt(ctx.mapIndex().INT().getText()))
                    .build();
        }

        if (ctx.mapValue() != null) {
            Object mapValue = null;
            if (ctx.mapValue().valueIdentifier().NAME_IDENTIFIER() != null) {
                mapValue = ctx.mapValue().valueIdentifier().NAME_IDENTIFIER().getText();
            } else if (ctx.mapValue().valueIdentifier().QUOTED_STRING() != null) {
                mapValue = ParsingUtils.getWithoutQuotes(ctx.mapValue().valueIdentifier().QUOTED_STRING().getText());
            } else if (ctx.mapValue().valueIdentifier().INT() != null) {
                mapValue = Integer.parseInt(ctx.mapValue().valueIdentifier().INT().getText());
            }

            return MapPart.builder()
                    .setMapValue(mapValue)
                    .build();
        }

        if (ctx.mapRank() != null) {
            String mapRank = ctx.mapRank().INT().getText();
            return MapPart.builder()
                    .setMapRank(Integer.parseInt(mapRank))
                    .build();
        }

        if (ctx.mapKeyRange() != null) {
            ConditionParser.StandardMapKeyRangeContext keyRange = ctx.mapKeyRange().standardMapKeyRange();
            ConditionParser.InvertedMapKeyRangeContext invertedKeyRange = ctx.mapKeyRange().invertedMapKeyRange();

            if (keyRange != null || invertedKeyRange != null) {
                ConditionParser.KeyRangeIdentifierContext range =
                        keyRange != null ? keyRange.keyRangeIdentifier() : invertedKeyRange.keyRangeIdentifier();
                boolean isInverted = keyRange == null;

                String startKey = range.mapKey(0).NAME_IDENTIFIER() != null
                        ? range.mapKey(0).NAME_IDENTIFIER().getText()
                        : ParsingUtils.getWithoutQuotes(range.mapKey(0).QUOTED_STRING().getText());

                String endKey = range.mapKey(1) != null
                        ? (range.mapKey(1).NAME_IDENTIFIER() != null
                        ? range.mapKey(1).NAME_IDENTIFIER().getText()
                        : ParsingUtils.getWithoutQuotes(range.mapKey(1).QUOTED_STRING().getText()))
                        : null;

                return MapPart.builder()
                        .setMapKeyRange(isInverted, startKey, endKey)
                        .build();
            }
        }

        if (ctx.mapKeyList() != null) {
            ConditionParser.StandardMapKeyListContext keyList = ctx.mapKeyList().standardMapKeyList();
            ConditionParser.InvertedMapKeyListContext invertedKeyList = ctx.mapKeyList().invertedMapKeyList();

            if (keyList != null || invertedKeyList != null) {
                ConditionParser.KeyListIdentifierContext list =
                        keyList != null ? keyList.keyListIdentifier() : invertedKeyList.keyListIdentifier();
                boolean isInverted = keyList == null;

                List<String> keyListStrings = list.mapKey().stream().map(
                        mapKey -> {
                            if (mapKey.NAME_IDENTIFIER() != null) {
                                return mapKey.NAME_IDENTIFIER().getText();
                            } else {
                                return ParsingUtils.getWithoutQuotes(mapKey.QUOTED_STRING().getText());
                            }
                        }
                ).toList();

                return MapPart.builder()
                        .setMapKeyList(isInverted, keyListStrings)
                        .build();
            }
        }

        if (ctx.mapIndexRange() != null) {
            ConditionParser.StandardMapIndexRangeContext indexRange = ctx.mapIndexRange().standardMapIndexRange();
            ConditionParser.InvertedMapIndexRangeContext invertedIndexRange = ctx.mapIndexRange().invertedMapIndexRange();

            if (indexRange != null || invertedIndexRange != null) {
                ConditionParser.IndexRangeIdentifierContext range =
                        indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
                boolean isInverted = indexRange == null;

                Integer start = Integer.parseInt(range.start().INT().getText());
                Integer count = null;
                if (range.count() != null) {
                    count = Integer.parseInt(range.count().INT().getText());
                }

                return MapPart.builder()
                        .setMapIndexRange(isInverted, start, count)
                        .build();
            }
        }

        if (ctx.mapValueList() != null) {
            ConditionParser.StandardMapValueListContext valueList = ctx.mapValueList().standardMapValueList();
            ConditionParser.InvertedMapValueListContext invertedValueList = ctx.mapValueList().invertedMapValueList();

            if (valueList != null || invertedValueList != null) {
                ConditionParser.ValueListIdentifierContext list =
                        valueList != null ? valueList.valueListIdentifier() : invertedValueList.valueListIdentifier();
                boolean isInverted = valueList == null;

                List<?> valueListObjects = list.valueIdentifier().stream().map(
                        listValue -> {
                            if (listValue.NAME_IDENTIFIER() != null) {
                                return listValue.NAME_IDENTIFIER().getText();
                            } else if (listValue.QUOTED_STRING() != null) {
                                return getWithoutQuotes(listValue.QUOTED_STRING().getText());
                            } else {
                                return Integer.parseInt(listValue.INT().getText());
                            }
                        }
                ).toList();

                return MapPart.builder()
                        .setMapValueList(isInverted, valueListObjects)
                        .build();
            }
        }

        if (ctx.mapValueRange() != null) {
            ConditionParser.StandardMapValueRangeContext valueRange = ctx.mapValueRange().standardMapValueRange();
            ConditionParser.InvertedMapValueRangeContext invertedValueRange = ctx.mapValueRange().invertedMapValueRange();

            if (valueRange != null || invertedValueRange != null) {
                ConditionParser.ValueRangeIdentifierContext range =
                        valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
                boolean isInverted = valueRange == null;

                Integer startValue = Integer.parseInt(range.valueIdentifier(0).INT().getText());

                Integer endValue = null;

                if (range.valueIdentifier(1) != null) {
                    if (range.valueIdentifier(1).INT() != null) {
                        endValue = Integer.parseInt(range.valueIdentifier(1).INT().getText());
                    }
                }

                return MapPart.builder()
                        .setMapValueRange(isInverted, startValue, endValue)
                        .build();
            }
        }

        if (ctx.mapRankRange() != null) {
            ConditionParser.StandardMapRankRangeContext rankRange = ctx.mapRankRange().standardMapRankRange();
            ConditionParser.InvertedMapRankRangeContext invertedRankRange = ctx.mapRankRange().invertedMapRankRange();

            if (rankRange != null || invertedRankRange != null) {
                ConditionParser.RankRangeIdentifierContext range =
                        rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
                boolean isInverted = rankRange == null;

                Integer start = Integer.parseInt(range.start().INT().getText());
                Integer count = null;
                if (range.count() != null) {
                    count = Integer.parseInt(range.count().INT().getText());
                }

                return MapPart.builder()
                        .setMapRankRange(isInverted, start, count)
                        .build();
            }
        }

        throw new AerospikeDSLException("Unexpected path type in a Map: %s".formatted(ctx.getText()));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
