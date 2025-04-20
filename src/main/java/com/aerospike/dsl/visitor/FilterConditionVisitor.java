//package com.aerospike.dsl.visitor;
//
//import com.aerospike.client.query.Filter;
//import com.aerospike.dsl.ConditionParser;
//import com.aerospike.dsl.exception.NoApplicableFilterException;
//import com.aerospike.dsl.model.AbstractPart;
//import com.aerospike.dsl.model.Expr;
//
//import java.util.Collection;
//import java.util.stream.Stream;
//
//import static com.aerospike.dsl.model.Expr.ExprPartsOperation.*;
//import static com.aerospike.dsl.visitor.VisitorUtils.FilterOperationType.*;
//import static com.aerospike.dsl.visitor.VisitorUtils.*;
//
//public class FilterConditionVisitor extends ExpressionConditionVisitor {
//
//    @Override
//    public AbstractPart visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        Filter filter = getFilterOrFail(left, right, GT);
//        return new Expr(filter);
//    }
//
//    @Override
//    public AbstractPart visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        Filter filter = getFilterOrFail(left, right, GTEQ);
//        return new Expr(filter);
//    }
//
//    @Override
//    public AbstractPart visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        Filter filter = getFilterOrFail(left, right, LT);
//        return new Expr(filter);
//    }
//
//    @Override
//    public AbstractPart visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        Filter filter = getFilterOrFail(left, right, LTEQ);
//        return new Expr(filter);
//    }
//
//    @Override
//    public AbstractPart visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        Filter filter = getFilterOrFail(left, right, EQ);
//        return new Expr(filter);
//    }
//
//    @Override
//    public AbstractPart visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
//        throw new NoApplicableFilterException("The operation is not supported by secondary index filter");
//    }
//
//    @Override
//    public AbstractPart visitAddExpression(ConditionParser.AddExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        validateNumericBinForFilter(left, right);
//        return new Expr(left, right, ADD);
//    }
//
//    @Override
//    public AbstractPart visitSubExpression(ConditionParser.SubExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        validateNumericBinForFilter(left, right);
//        return new Expr(left, right, SUB);
//    }
//
//    @Override
//    public AbstractPart visitDivExpression(ConditionParser.DivExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        validateNumericBinForFilter(left, right);
//        return new Expr(left, right, DIV);
//    }
//
//    @Override
//    public AbstractPart visitMulExpression(ConditionParser.MulExpressionContext ctx) {
//        AbstractPart left = visit(ctx.operand(0));
//        AbstractPart right = visit(ctx.operand(1));
//
//        validateNumericBinForFilter(left, right);
//        return new Expr(left, right, MUL);
//    }
//
//    @Override
//    public AbstractPart visitAndExpression(ConditionParser.AndExpressionContext ctx) {
//        Expr left = (Expr) visit(ctx.expression(0));
//        Expr right = (Expr) visit(ctx.expression(1));
//
//        logicalSetBinsAsBooleanExpr(left, right);
//        Collection<Filter> filters = Stream.concat(
//                        left.getSIndexFilter().getFilter().stream(),
//                        right.getSIndexFilter().getFilter().stream()
//                ).toList();
//        return new Expr(new SIndexFilter(filters));
//    }
//
//    @Override
//    public AbstractPart visitOrExpression(ConditionParser.OrExpressionContext ctx) {
//        Expr left = (Expr) visit(ctx.expression(0));
//        Expr right = (Expr) visit(ctx.expression(1));
//
//        logicalSetBinsAsBooleanExpr(left, right);
//        Collection<Filter> filters = Stream.concat(
//                left.getSIndexFilter().getFilter().stream(),
//                right.getSIndexFilter().getFilter().stream()
//        ).toList();
//        return new Expr(new SIndexFilter(filters));
//    }
//}
