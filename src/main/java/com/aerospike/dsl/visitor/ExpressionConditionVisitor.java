package com.aerospike.dsl.visitor;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.ConditionBaseVisitor;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.index.Index;
import com.aerospike.dsl.model.*;
import com.aerospike.dsl.model.cdt.list.ListIndex;
import com.aerospike.dsl.model.cdt.list.ListIndexRange;
import com.aerospike.dsl.model.cdt.list.ListRank;
import com.aerospike.dsl.model.cdt.list.ListRankRange;
import com.aerospike.dsl.model.cdt.list.ListRankRangeRelative;
import com.aerospike.dsl.model.cdt.list.ListTypeDesignator;
import com.aerospike.dsl.model.cdt.list.ListValue;
import com.aerospike.dsl.model.cdt.list.ListValueList;
import com.aerospike.dsl.model.cdt.list.ListValueRange;
import com.aerospike.dsl.model.cdt.map.*;
import com.aerospike.dsl.util.TypeUtils;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import static com.aerospike.dsl.model.Expr.ExprPartsOperation.*;
import static com.aerospike.dsl.util.ParsingUtils.unquote;
import static com.aerospike.dsl.visitor.VisitorUtils.*;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<AbstractPart> {

    private final boolean isFilterExp;
    private final boolean isSIndexFilter;
    private final String namespace;
    private final Collection<Index> indexes;

    public ExpressionConditionVisitor() {
        this.isFilterExp = false;
        this.isSIndexFilter = false;
        this.namespace = null;
        this.indexes = null;
    }

    public ExpressionConditionVisitor(boolean isFilterExp, boolean isSIndexFilter) {
        this.isFilterExp = isFilterExp;
        this.isSIndexFilter = isSIndexFilter;
        this.namespace = null;
        this.indexes = null;
    }

    public ExpressionConditionVisitor(String namespace, Collection<Index> indexes) {
        this.isFilterExp = false;
        this.isSIndexFilter = false;
        this.namespace = namespace;
        this.indexes = indexes;
    }

    public ExpressionConditionVisitor(String namespace, Collection<Index> indexes, boolean isFilterExp,
                                      boolean isSIndexFilter) {
        this.isFilterExp = isFilterExp;
        this.isSIndexFilter = isSIndexFilter;
        this.namespace = namespace;
        this.indexes = indexes;
    }

    @Override
    public AbstractPart visitWithExpression(ConditionParser.WithExpressionContext ctx) {
        List<WithOperand> expressions = new ArrayList<>();

        // iterate each definition
        for (ConditionParser.VariableDefinitionContext vdc : ctx.variableDefinition()) {
            AbstractPart part = visit(vdc.expression());
            WithOperand withOperand = new WithOperand(part, vdc.stringOperand().getText());
            expressions.add(withOperand);
        }
        // last expression is the action (described after "do")
        expressions.add(new WithOperand(visit(ctx.expression()), true));
        return new Expr(new WithOperandsExpr(expressions), Expr.ExprPartsOperation.WITH);
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
        return new Expr(left, right, Expr.ExprPartsOperation.AND);
    }

    @Override
    public AbstractPart visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        Expr left = (Expr) visit(ctx.expression(0));
        Expr right = (Expr) visit(ctx.expression(1));

        logicalSetBinsAsBooleanExpr(left, right);
        return new Expr(left, right, Expr.ExprPartsOperation.OR);
    }

    @Override
    public AbstractPart visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        Expr expr = (Expr) visit(ctx.expression());

        logicalSetBinAsBooleanExpr(expr);
        return new Expr(expr, Expr.ExprPartsOperation.LT); // TODO: unary operator
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

    @Override
    public AbstractPart visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, Expr.ExprPartsOperation.GT);
    }

    @Override
    public AbstractPart visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, Expr.ExprPartsOperation.GTEQ);
    }

    @Override
    public AbstractPart visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, Expr.ExprPartsOperation.LT);
    }

    @Override
    public AbstractPart visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, Expr.ExprPartsOperation.LTEQ);
    }

    @Override
    public AbstractPart visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, Expr.ExprPartsOperation.EQ);
    }

    @Override
    public AbstractPart visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, Expr.ExprPartsOperation.NOTEQ);
    }

    @Override
    public AbstractPart visitAddExpression(ConditionParser.AddExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        if (!isFilterExp) validateNumericBinForFilter(left, right);
        return new Expr(left, right, ADD);
    }

    @Override
    public AbstractPart visitSubExpression(ConditionParser.SubExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        if (!isFilterExp) validateNumericBinForFilter(left, right);
        return new Expr(left, right, SUB);
    }

    @Override
    public AbstractPart visitMulExpression(ConditionParser.MulExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        if (!isFilterExp) validateNumericBinForFilter(left, right);
        return new Expr(left, right, MUL);
    }

    @Override
    public AbstractPart visitDivExpression(ConditionParser.DivExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        if (!isFilterExp) validateNumericBinForFilter(left, right);
        return new Expr(left, right, DIV);
    }

    @Override
    public AbstractPart visitModExpression(ConditionParser.ModExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, MOD);
    }

    @Override
    public AbstractPart visitIntAndExpression(ConditionParser.IntAndExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, INT_AND);
    }

    @Override
    public AbstractPart visitIntOrExpression(ConditionParser.IntOrExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, INT_OR);
    }

    @Override
    public AbstractPart visitIntXorExpression(ConditionParser.IntXorExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, INT_XOR);
    }

    @Override
    public AbstractPart visitIntNotExpression(ConditionParser.IntNotExpressionContext ctx) {
        AbstractPart operand = visit(ctx.operand());

        return new Expr(operand, INT_NOT);
    }

    @Override
    public AbstractPart visitIntLShiftExpression(ConditionParser.IntLShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, L_SHIFT);
    }

    @Override
    public AbstractPart visitIntRShiftExpression(ConditionParser.IntRShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        return new Expr(left, right, R_SHIFT);
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
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ParseTree child = listNode.getChild(i);
            if (!shouldVisitListElement(i, size, child)) {
                continue;
            }

            AbstractPart operand = visit(child); // delegate to a dedicated visitor
            if (operand == null) {
                throw new AerospikeDSLException("Unable to parse list operand");
            }

            try {
                list.add(((ParsedOperand) operand).getValue());
            } catch (ClassCastException e) {
                throw new AerospikeDSLException("List constant contains elements of different type");
            }
        }

        return new ListOperand(list);
    }

    @Override
    public AbstractPart visitOrderedMapConstant(ConditionParser.OrderedMapConstantContext ctx) {
        return readChildrenIntoMapOperand(ctx);
    }

    public TreeMap<Object, Object> getOrderedMapPair(ParseTree ctx) {
        if (ctx.getChild(0) == null || ctx.getChild(2) == null) {
            throw new AerospikeDSLException("Unable to parse map operand");
        }
        Object key = ((ParsedOperand) visit(ctx.getChild(0))).getValue();
        Object value = ((ParsedOperand) visit(ctx.getChild(2))).getValue();
        TreeMap<Object, Object> map = new TreeMap<>();
        map.put(key, value);
        return map;
    }

    public MapOperand readChildrenIntoMapOperand(RuleNode mapNode) {
        int size = mapNode.getChildCount();
        TreeMap<Object, Object> map = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            ParseTree child = mapNode.getChild(i);
            if (!shouldVisitMapElement(i, size, child)) {
                continue;
            }

            TreeMap<Object, Object> mapOfPair = getOrderedMapPair(child); // delegate to a dedicated visitor

            try {
                mapOfPair.forEach(map::putIfAbsent); // put contents of the current map pair to the resulting map
            } catch (ClassCastException e) {
                throw new AerospikeDSLException("Map constant contains elements of different type");
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
                if (implicitType == null) {
                    implicitType = Exp.Type.INT;
                }
                ((BinPart) part).updateExp(implicitType);
            } else { // ListPart or MapPart
                if (implicitType == null) {
                    implicitType = TypeUtils.getDefaultType(part);
                }
                part.setExpType(implicitType);
            }
        }
        return part;
    }

    @Override
    public AbstractPart visitPath(ConditionParser.PathContext ctx) {
        BasePath basePath = (BasePath) visit(ctx.basePath());
        List<AbstractPart> parts = basePath.getParts();

        // if there are other parts except bin, get a corresponding Exp
        if (!parts.isEmpty() || ctx.pathFunction() != null && ctx.pathFunction().pathFunctionCount() != null) {
                Exp exp = PathOperand.processPath(basePath, ctx.pathFunction() == null
                    ? null
                    : (PathFunction) visit(ctx.pathFunction()));
            return exp == null ? null : new PathOperand(exp);
        }
        return basePath.getBinPart();
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
        throw new AerospikeDSLException("Unexpected list part: %s".formatted(ctx.getText()));
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
        throw new AerospikeDSLException("Unexpected map part: %s".formatted(ctx.getText()));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }

    @Override
    public AbstractPart visitChildren(RuleNode node) {
        AbstractPart result = defaultResult();
        int n = node.getChildCount();
        for (int i=0; i<n; i++) {
            if (!shouldVisitNextChild(node, result)) {
                break;
            }

            ParseTree c = node.getChild(i);
            AbstractPart childResult = c.accept(this);
            result = aggregateResult(result, childResult);
        }

        if (result != null && result.getPartType() == AbstractPart.PartType.EXPR) {
            return buildExpr((Expr) result, namespace, indexes, isFilterExp);
        }
        return result;
    }
}
