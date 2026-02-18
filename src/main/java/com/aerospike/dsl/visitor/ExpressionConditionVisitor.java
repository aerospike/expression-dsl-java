package com.aerospike.dsl.visitor;

import com.aerospike.dsl.ConditionBaseVisitor;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.ExpressionContainer;
import com.aerospike.dsl.parts.cdt.list.ListIndex;
import com.aerospike.dsl.parts.cdt.list.ListIndexRange;
import com.aerospike.dsl.parts.cdt.list.ListRank;
import com.aerospike.dsl.parts.cdt.list.ListRankRange;
import com.aerospike.dsl.parts.cdt.list.ListRankRangeRelative;
import com.aerospike.dsl.parts.cdt.list.ListTypeDesignator;
import com.aerospike.dsl.parts.cdt.list.ListValue;
import com.aerospike.dsl.parts.cdt.list.ListValueList;
import com.aerospike.dsl.parts.cdt.list.ListValueRange;
import com.aerospike.dsl.parts.cdt.map.*;
import com.aerospike.dsl.parts.controlstructure.AndStructure;
import com.aerospike.dsl.parts.controlstructure.ExclusiveStructure;
import com.aerospike.dsl.parts.controlstructure.OrStructure;
import com.aerospike.dsl.parts.controlstructure.WhenStructure;
import com.aerospike.dsl.parts.controlstructure.WithStructure;
import com.aerospike.dsl.parts.operand.*;
import com.aerospike.dsl.parts.path.BasePath;
import com.aerospike.dsl.parts.path.BinPart;
import com.aerospike.dsl.parts.path.Path;
import com.aerospike.dsl.parts.path.PathFunction;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.aerospike.dsl.util.ParsingUtils.*;
import static com.aerospike.dsl.visitor.VisitorUtils.*;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<AbstractPart> {

    @Override
    public AbstractPart visitWithExpression(ConditionParser.WithExpressionContext ctx) {
        List<WithOperand> expressions = new ArrayList<>();

        // iterate through each definition
        for (ConditionParser.VariableDefinitionContext vdc : ctx.variableDefinition()) {
            AbstractPart part = visit(vdc.expression());
            WithOperand withOperand = new WithOperand(part, vdc.stringOperand().getText());
            expressions.add(withOperand);
        }
        // last expression is the action (described after "do")
        expressions.add(new WithOperand(visit(ctx.expression()), true));
        return new ExpressionContainer(new WithStructure(expressions),
                ExpressionContainer.ExprPartsOperation.WITH_STRUCTURE);
    }

    @Override
    public AbstractPart visitWhenExpression(ConditionParser.WhenExpressionContext ctx) {
        List<AbstractPart> parts = new ArrayList<>();
        // iterate through each definition declaration
        for (ConditionParser.ExpressionMappingContext emc : ctx.expressionMapping()) {
            // visit condition
            parts.add(visit(emc.expression(0)));
            // visit action
            parts.add(visit(emc.expression(1)));
        }
        // visit default
        parts.add(visit(ctx.expression()));
        return new ExpressionContainer(new WhenStructure(parts), ExpressionContainer.ExprPartsOperation.WHEN_STRUCTURE);
    }

    @Override
    public AbstractPart visitAndExpression(ConditionParser.AndExpressionContext ctx) {
        // If there's only one basicExpression and no 'and' operators, just pass through
        if (ctx.basicExpression().size() == 1) {
            return visit(ctx.basicExpression(0));
        }

        List<ExpressionContainer> expressions = new ArrayList<>();
        // iterate through each sub-expression
        for (ConditionParser.BasicExpressionContext ec : ctx.basicExpression()) {
            ExpressionContainer expr = (ExpressionContainer) visit(ec);
            if (expr == null) return null;

            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr);
        }
        return new ExpressionContainer(new AndStructure(expressions), ExpressionContainer.ExprPartsOperation.AND_STRUCTURE);
    }

    @Override
    public AbstractPart visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        // If there's only one andExpression and no 'or' operators, just pass through
        if (ctx.logicalAndExpression().size() == 1) {
            return visit(ctx.logicalAndExpression(0));
        }

        List<ExpressionContainer> expressions = new ArrayList<>();
        // iterate through each sub-expression
        for (ConditionParser.LogicalAndExpressionContext ec : ctx.logicalAndExpression()) {
            ExpressionContainer expr = (ExpressionContainer) visit(ec);
            if (expr == null) return null;

            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr);
        }
        return new ExpressionContainer(new OrStructure(expressions), ExpressionContainer.ExprPartsOperation.OR_STRUCTURE);
    }

    @Override
    public AbstractPart visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        ExpressionContainer expr = (ExpressionContainer) visit(ctx.expression());

        logicalSetBinAsBooleanExpr(expr);
        return new ExpressionContainer(expr, ExpressionContainer.ExprPartsOperation.NOT);
    }

    @Override
    public AbstractPart visitExclusiveExpression(ConditionParser.ExclusiveExpressionContext ctx) {
        if (ctx.expression().size() < 2) {
            throw new DslParseException("Exclusive logical operator requires 2 or more expressions");
        }
        List<ExpressionContainer> expressions = new ArrayList<>();
        // iterate through each sub-expression
        for (ConditionParser.ExpressionContext ec : ctx.expression()) {
            ExpressionContainer expr = (ExpressionContainer) visit(ec);
            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr);
        }
        return new ExpressionContainer(new ExclusiveStructure(expressions),
                ExpressionContainer.ExprPartsOperation.EXCLUSIVE_STRUCTURE);
    }

    @Override
    public AbstractPart visitComparisonExpressionWrapper(ConditionParser.ComparisonExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.comparisonExpression());
    }

    @Override
    public AbstractPart visitAdditiveExpressionWrapper(ConditionParser.AdditiveExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.additiveExpression());
    }

    @Override
    public AbstractPart visitMultiplicativeExpressionWrapper(ConditionParser.MultiplicativeExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.multiplicativeExpression());
    }

    @Override
    public AbstractPart visitBitwiseExpressionWrapper(ConditionParser.BitwiseExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.bitwiseExpression());
    }

    @Override
    public AbstractPart visitShiftExpressionWrapper(ConditionParser.ShiftExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.shiftExpression());
    }

    @Override
    public AbstractPart visitUnaryExpressionWrapper(ConditionParser.UnaryExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.unaryExpression());
    }

    @Override
    public AbstractPart visitUnaryPlusExpression(ConditionParser.UnaryPlusExpressionContext ctx) {
        // Unary '+' is a no-op, delegate to the inner expression
        return visit(ctx.unaryExpression());
    }

    @Override
    public AbstractPart visitUnaryMinusExpression(ConditionParser.UnaryMinusExpressionContext ctx) {
        AbstractPart operand = visit(ctx.unaryExpression());

        // Negate literal operands directly
        if (operand instanceof IntOperand intOp) {
            return new IntOperand(-intOp.getValue());
        }
        if (operand instanceof FloatOperand floatOp) {
            return new FloatOperand(-floatOp.getValue());
        }

        // For type-cast expressions (TO_INT, TO_FLOAT) with a literal operand,
        // push negation into the literal to preserve Exp tree equivalence (e.g., -5.asFloat())
        if (operand instanceof ExpressionContainer container && container.isUnary()) {
            ExpressionContainer.ExprPartsOperation op = container.getOperationType();
            if (op == ExpressionContainer.ExprPartsOperation.TO_INT
                    || op == ExpressionContainer.ExprPartsOperation.TO_FLOAT) {
                AbstractPart inner = container.getLeft();
                if (inner instanceof IntOperand intOp) {
                    return new ExpressionContainer(new IntOperand(-intOp.getValue()), op);
                }
                if (inner instanceof FloatOperand floatOp) {
                    return new ExpressionContainer(new FloatOperand(-floatOp.getValue()), op);
                }
            }
        }

        // General case: negate via 0 - operand
        return new ExpressionContainer(new IntOperand(0L), operand, ExpressionContainer.ExprPartsOperation.SUB);
    }

    @Override
    public AbstractPart visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression(0));
        AbstractPart right = visit(ctx.additiveExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.GT);
    }

    @Override
    public AbstractPart visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression(0));
        AbstractPart right = visit(ctx.additiveExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.GTEQ);
    }

    @Override
    public AbstractPart visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression(0));
        AbstractPart right = visit(ctx.additiveExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.LT);
    }

    @Override
    public AbstractPart visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression(0));
        AbstractPart right = visit(ctx.additiveExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.LTEQ);
    }

    @Override
    public AbstractPart visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression(0));
        AbstractPart right = visit(ctx.additiveExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.EQ);
    }

    @Override
    public AbstractPart visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression(0));
        AbstractPart right = visit(ctx.additiveExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.NOTEQ);
    }

    @Override
    public AbstractPart visitAddExpression(ConditionParser.AddExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression());
        AbstractPart right = visit(ctx.multiplicativeExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.ADD);
    }

    @Override
    public AbstractPart visitSubExpression(ConditionParser.SubExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression());
        AbstractPart right = visit(ctx.multiplicativeExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.SUB);
    }

    @Override
    public AbstractPart visitMulExpression(ConditionParser.MulExpressionContext ctx) {
        AbstractPart left = visit(ctx.multiplicativeExpression());
        AbstractPart right = visit(ctx.powerExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.MUL);
    }

    @Override
    public AbstractPart visitDivExpression(ConditionParser.DivExpressionContext ctx) {
        AbstractPart left = visit(ctx.multiplicativeExpression());
        AbstractPart right = visit(ctx.powerExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.DIV);
    }

    @Override
    public AbstractPart visitModExpression(ConditionParser.ModExpressionContext ctx) {
        AbstractPart left = visit(ctx.multiplicativeExpression()); // first operand
        AbstractPart right = visit(ctx.powerExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.MOD);
    }

    @Override
    public AbstractPart visitPowerExpressionWrapper(ConditionParser.PowerExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.powerExpression());
    }

    @Override
    public AbstractPart visitPowExpression(ConditionParser.PowExpressionContext ctx) {
        AbstractPart left = visit(ctx.powerExpression(0));
        AbstractPart right = visit(ctx.powerExpression(1));

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.POW);
    }

    @Override
    public AbstractPart visitFunctionCall(ConditionParser.FunctionCallContext ctx) {
        // If error recovery created this node without actual parentheses, fail fast
        if (ctx.getChildCount() < 3 || !ctx.getChild(1).getText().equals("(")) {
            throw new DslParseException("Unexpected identifier: " + ctx.getChild(0).getText());
        }

        String funcName = ctx.NAME_IDENTIFIER().getText();
        List<AbstractPart> args = new ArrayList<>();
        for (ConditionParser.ExpressionContext ec : ctx.expression()) {
            args.add(visit(ec));
        }

        return switch (funcName) {
            // Unary arithmetic functions
            case "abs" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0), ExpressionContainer.ExprPartsOperation.ABS);
            }
            case "ceil" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0), ExpressionContainer.ExprPartsOperation.CEIL);
            }
            case "floor" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0), ExpressionContainer.ExprPartsOperation.FLOOR);
            }
            // Binary arithmetic functions
            case "log" -> {
                validateFunctionArgCount(funcName, args, 2);
                yield new ExpressionContainer(args.get(0), args.get(1),
                        ExpressionContainer.ExprPartsOperation.LOG);
            }
            // Variadic arithmetic functions
            case "min" -> {
                validateFunctionArgCountAtLeast(funcName, args, 2);
                yield new ExpressionContainer(new FunctionArgs(args),
                        ExpressionContainer.ExprPartsOperation.MIN_FUNC);
            }
            case "max" -> {
                validateFunctionArgCountAtLeast(funcName, args, 2);
                yield new ExpressionContainer(new FunctionArgs(args),
                        ExpressionContainer.ExprPartsOperation.MAX_FUNC);
            }
            // Bit-scanning functions
            case "countOneBits" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0),
                        ExpressionContainer.ExprPartsOperation.COUNT_ONE_BITS);
            }
            case "findBitLeft" -> {
                validateFunctionArgCount(funcName, args, 2);
                yield new ExpressionContainer(args.get(0), args.get(1),
                        ExpressionContainer.ExprPartsOperation.FIND_BIT_LEFT);
            }
            case "findBitRight" -> {
                validateFunctionArgCount(funcName, args, 2);
                yield new ExpressionContainer(args.get(0), args.get(1),
                        ExpressionContainer.ExprPartsOperation.FIND_BIT_RIGHT);
            }
            default -> throw new DslParseException("Unknown function: " + funcName);
        };
    }

    private void validateFunctionArgCount(String funcName, List<AbstractPart> args, int expected) {
        if (args.size() != expected) {
            throw new DslParseException(
                    "Function '%s' expects %d argument(s), got %d".formatted(funcName, expected, args.size()));
        }
    }

    private void validateFunctionArgCountAtLeast(String funcName, List<AbstractPart> args, int minCount) {
        if (args.size() < minCount) {
            throw new DslParseException(
                    "Function '%s' expects at least %d arguments, got %d".formatted(funcName, minCount, args.size()));
        }
    }

    @Override
    public AbstractPart visitIntAndExpression(ConditionParser.IntAndExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression()); // first operand
        AbstractPart right = visit(ctx.shiftExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.INT_AND);
    }

    @Override
    public AbstractPart visitIntOrExpression(ConditionParser.IntOrExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression()); // first operand
        AbstractPart right = visit(ctx.shiftExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.INT_OR);
    }

    @Override
    public AbstractPart visitIntXorExpression(ConditionParser.IntXorExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression()); // first operand
        AbstractPart right = visit(ctx.shiftExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.INT_XOR);
    }

    @Override
    public AbstractPart visitIntNotExpression(ConditionParser.IntNotExpressionContext ctx) {
        AbstractPart operand = visit(ctx.shiftExpression());

        return new ExpressionContainer(operand, ExpressionContainer.ExprPartsOperation.INT_NOT);
    }

    @Override
    public AbstractPart visitIntLShiftExpression(ConditionParser.IntLShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.shiftExpression()); // first operand
        AbstractPart right = visit(ctx.unaryExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.L_SHIFT);
    }

    @Override
    public AbstractPart visitIntRShiftExpression(ConditionParser.IntRShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.shiftExpression()); // first operand
        AbstractPart right = visit(ctx.unaryExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.R_SHIFT);
    }

    @Override
    public AbstractPart visitIntLogicalRShiftExpression(ConditionParser.IntLogicalRShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.shiftExpression()); // first operand
        AbstractPart right = visit(ctx.unaryExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.LOGICAL_R_SHIFT);
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
        // todo: TYPE_PARAM?
        return new PathFunction(PathFunction.PathFunctionType.COUNT, PathFunction.ReturnParam.COUNT, null);
    }

    @Override
    public AbstractPart visitPathFunctionCast(ConditionParser.PathFunctionCastContext ctx) {
        String typeVal = extractTypeFromMethod(ctx.PATH_FUNCTION_CAST().getText());
        PathFunction.CastType castType = PathFunction.CastType.valueOf(typeVal.toUpperCase());
        Exp.Type binType = PathFunction.castTypeToExpType(castType);

        return new PathFunction(PathFunction.PathFunctionType.CAST, null, binType);
    }

    @Override
    public AbstractPart visitOperandCast(ConditionParser.OperandCastContext ctx) {
        AbstractPart numberOperand = visit(ctx.numberOperand());

        String castText = ctx.pathFunctionCast().PATH_FUNCTION_CAST().getText();
        String typeVal = extractTypeFromMethod(castText);
        PathFunction.CastType castType = PathFunction.CastType.valueOf(typeVal.toUpperCase());

        ExpressionContainer.ExprPartsOperation op = switch (castType) {
            case INT -> ExpressionContainer.ExprPartsOperation.TO_INT;
            case FLOAT -> ExpressionContainer.ExprPartsOperation.TO_FLOAT;
        };

        return new ExpressionContainer(numberOperand, op);
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

    @Override
    public AbstractPart visitBinPart(ConditionParser.BinPartContext ctx) {
        return new BinPart(ctx.NAME_IDENTIFIER().getText());
    }

    @Override
    public AbstractPart visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }

    @Override
    public AbstractPart visitListConstant(ConditionParser.ListConstantContext ctx) {
        return readChildrenIntoListOperand(ctx);
    }

    public ListOperand readChildrenIntoListOperand(RuleNode listNode) {
        int size = listNode.getChildCount();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ParseTree child = listNode.getChild(i);
            if (!shouldVisitListElement(i, size, child)) {
                continue;
            }

            AbstractPart operand = visit(child); // delegate to a dedicated visitor
            if (operand == null) {
                throw new DslParseException("Unable to parse list operand");
            }

            try {
                values.add(((ParsedValueOperand) operand).getValue());
            } catch (ClassCastException e) {
                throw new DslParseException("List constant contains elements of different type");
            }
        }

        return new ListOperand(values);
    }

    @Override
    public AbstractPart visitOrderedMapConstant(ConditionParser.OrderedMapConstantContext ctx) {
        return readChildrenIntoMapOperand(ctx);
    }

    public SortedMap<Object, Object> getOrderedMapPair(ParseTree ctx) {
        if (ctx.getChild(0) == null || ctx.getChild(2) == null) {
            throw new DslParseException("Unable to parse map operand");
        }

        Object key = ((ParsedValueOperand) visit(ctx.getChild(0))).getValue();
        Object value = ((ParsedValueOperand) visit(ctx.getChild(2))).getValue();

        SortedMap<Object, Object> map = new TreeMap<>();
        map.put(key, value);

        return map;
    }

    public MapOperand readChildrenIntoMapOperand(RuleNode mapNode) {
        int size = mapNode.getChildCount();
        SortedMap<Object, Object> map = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            ParseTree child = mapNode.getChild(i);
            if (!shouldVisitMapElement(i, size, child)) {
                continue;
            }

            SortedMap<Object, Object> mapOfPair = getOrderedMapPair(child); // delegate to a dedicated visitor

            try {
                mapOfPair.forEach(map::putIfAbsent); // put contents of the current map pair to the resulting map
            } catch (ClassCastException e) {
                throw new DslParseException("Map constant contains elements of different type");
            }
        }

        return new MapOperand(map);
    }

    @Override
    public AbstractPart visitStringOperand(ConditionParser.StringOperandContext ctx) {
        String text = unquote(ctx.getText());
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
        return new IntOperand(parseIntLiteral(text));
    }

    /**
     * Parses an unsigned INT literal which may be decimal, hexadecimal (0x/0X), or binary (0b/0B).
     * Signs are handled at the parser level by the unaryExpression rule.
     */
    private static long parseIntLiteral(String text) {
        try {
            if (text.length() > 2 && text.charAt(0) == '0') {
                char prefix = text.charAt(1);
                if (prefix == 'x' || prefix == 'X') {
                    return Long.parseLong(text.substring(2), 16);
                } else if (prefix == 'b' || prefix == 'B') {
                    return Long.parseLong(text.substring(2), 2);
                }
            }
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new DslParseException("Invalid integer literal: " + text, e);
        }
    }

    /**
     * Parses unsigned FLOAT literals and leading-dot floats (e.g., ".37").
     * Signs are handled at the parser level by the unaryExpression rule.
     */
    @Override
    public AbstractPart visitFloatOperand(ConditionParser.FloatOperandContext ctx) {
        try {
            if (ctx.FLOAT() != null) {
                return new FloatOperand(Double.parseDouble(ctx.FLOAT().getText()));
            }
            // Leading-dot float: ".37" → 0.37
            return new FloatOperand(Double.parseDouble("0." + ctx.INT().getText()));
        } catch (NumberFormatException e) {
            throw new DslParseException("Invalid float literal: " + ctx.getText(), e);
        }
    }

    @Override
    public AbstractPart visitBooleanOperand(ConditionParser.BooleanOperandContext ctx) {
        String text = ctx.getText();
        return new BooleanOperand(Boolean.parseBoolean(text));
    }

    @Override
    public AbstractPart visitPlaceholder(ConditionParser.PlaceholderContext ctx) {
        // Extract index from the placeholder
        String placeholderText = ctx.getText();
        int index = Integer.parseInt(placeholderText.substring(1));
        return new PlaceholderOperand(index);
    }

    @Override
    public AbstractPart visitBasePath(ConditionParser.BasePathContext ctx) {
        BinPart binPart = null;
        List<AbstractPart> cdtParts = new ArrayList<>();
        List<ParseTree> ctxChildrenExclDots = ctx.children.stream()
                .filter(tree -> !tree.getText().equals("."))
                .toList();

        for (ParseTree child : ctxChildrenExclDots) {
            AbstractPart part = visit(child);
            switch (part.getPartType()) {
                case BIN_PART -> binPart = (BinPart) part;
                case LIST_PART, MAP_PART -> cdtParts.add(part);
                default -> throw new DslParseException("Unexpected path part: %s".formatted(part.getPartType()));
            }
        }

        if (binPart == null) {
            throw new DslParseException("Expecting bin to be the first path part from the left");
        }

        return new BasePath(binPart, cdtParts);
    }

    @Override
    public AbstractPart visitVariable(ConditionParser.VariableContext ctx) {
        String text = ctx.VARIABLE_REFERENCE().getText();
        return new VariableOperand(extractVariableNameOrFail(text));
    }

    @Override
    public AbstractPart visitPath(ConditionParser.PathContext ctx) {
        BasePath basePath = (BasePath) visit(ctx.basePath());
        List<AbstractPart> cdtParts = basePath.getCdtParts();
        overrideWithPathFunction(basePath.getBinPart(), ctx);

        // if there are other parts except bin, get a corresponding Exp
        if (!cdtParts.isEmpty() || ctx.pathFunction() != null && ctx.pathFunction().pathFunctionCount() != null) {
            return new Path(basePath, ctx.pathFunction() == null
                    ? null
                    : (PathFunction) visit(ctx.pathFunction()));
        }
        return basePath.getBinPart();
    }

    private void overrideWithPathFunction(BinPart binPart, ConditionParser.PathContext ctx) {
        ConditionParser.PathFunctionContext pathFunctionContext = ctx.pathFunction();

        // Override with path function (explicit get or cast)
        if (pathFunctionContext != null) {
            PathFunction pathFunction = (PathFunction) visit(pathFunctionContext);
            if (pathFunction != null) {
                Exp.Type type = pathFunction.getBinType();
                if (type != null) {
                    binPart.updateExp(type);
                    binPart.setTypeExplicitlySet(true);
                }
            }
        }
    }

    @Override
    public AbstractPart visitListPart(ConditionParser.ListPartContext ctx) {
        if (ctx.LIST_TYPE_DESIGNATOR() != null) return ListTypeDesignator.from();
        if (ctx.listIndex() != null) return ListIndex.from(ctx.listIndex());
        if (ctx.listValue() != null) return ListValue.from(ctx.listValue());
        if (ctx.listRank() != null) return ListRank.from(ctx.listRank());
        if (ctx.listIndexRange() != null) return ListIndexRange.from(ctx.listIndexRange());
        if (ctx.listValueList() != null) return ListValueList.from(ctx.listValueList());
        if (ctx.listValueRange() != null) return ListValueRange.from(ctx.listValueRange());
        if (ctx.listRankRange() != null) return ListRankRange.from(ctx.listRankRange());
        if (ctx.listRankRangeRelative() != null)
            return ListRankRangeRelative.from(ctx.listRankRangeRelative());
        throw new DslParseException("Unexpected list part: %s".formatted(ctx.getText()));
    }

    @Override
    public AbstractPart visitMapPart(ConditionParser.MapPartContext ctx) {
        if (ctx.MAP_TYPE_DESIGNATOR() != null) return MapTypeDesignator.from();
        if (ctx.mapKey() != null) return MapKey.from(ctx.mapKey());
        if (ctx.mapIndex() != null) return MapIndex.from(ctx.mapIndex());
        if (ctx.mapValue() != null) return MapValue.from(ctx.mapValue());
        if (ctx.mapRank() != null) return MapRank.from(ctx.mapRank());
        if (ctx.mapKeyRange() != null) return MapKeyRange.from(ctx.mapKeyRange());
        if (ctx.mapKeyList() != null) return MapKeyList.from(ctx.mapKeyList());
        if (ctx.mapIndexRange() != null) return MapIndexRange.from(ctx.mapIndexRange());
        if (ctx.mapValueList() != null) return MapValueList.from(ctx.mapValueList());
        if (ctx.mapValueRange() != null) return MapValueRange.from(ctx.mapValueRange());
        if (ctx.mapRankRange() != null) return MapRankRange.from(ctx.mapRankRange());
        if (ctx.mapRankRangeRelative() != null)
            return MapRankRangeRelative.from(ctx.mapRankRangeRelative());
        if (ctx.mapIndexRangeRelative() != null)
            return MapIndexRangeRelative.from(ctx.mapIndexRangeRelative());
        throw new DslParseException("Unexpected map part: %s".formatted(ctx.getText()));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
