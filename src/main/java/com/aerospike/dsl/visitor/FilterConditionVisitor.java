package com.aerospike.dsl.visitor;

import com.aerospike.client.query.Filter;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.Expr;
import com.aerospike.dsl.model.SIndexFilter;

import static com.aerospike.dsl.model.Expr.ExprPartsOperation.*;
import static com.aerospike.dsl.visitor.VisitorUtils.FilterOperationType.*;
import static com.aerospike.dsl.visitor.VisitorUtils.getFilterOrFail;
import static com.aerospike.dsl.visitor.VisitorUtils.validateNumericBin;

public class FilterConditionVisitor extends ExpressionConditionVisitor {

    @Override
    public AbstractPart visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Filter filter = getFilterOrFail(left, right, GT);
        return new Expr(new SIndexFilter(filter));
    }

    @Override
    public AbstractPart visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Filter filter = getFilterOrFail(left, right, GTEQ);
        return new Expr(new SIndexFilter(filter));
    }

    @Override
    public AbstractPart visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Filter filter = getFilterOrFail(left, right, LT);
        return new Expr(new SIndexFilter(filter));
    }

    @Override
    public AbstractPart visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Filter filter = getFilterOrFail(left, right, LTEQ);
        return new Expr(new SIndexFilter(filter));
    }

    @Override
    public AbstractPart visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        Filter filter = getFilterOrFail(left, right, EQ);
        return new Expr(new SIndexFilter(filter));
    }

    @Override
    public AbstractPart visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        throw new AerospikeDSLException("The operation is not supported by secondary index filter");
    }

    @Override
    public AbstractPart visitAddExpression(ConditionParser.AddExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        validateNumericBin(left, right);
        return new Expr(left, right, ADD);
    }

    @Override
    public AbstractPart visitSubExpression(ConditionParser.SubExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        validateNumericBin(left, right);
        return new Expr(left, right, SUB);
    }

    @Override
    public AbstractPart visitDivExpression(ConditionParser.DivExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        validateNumericBin(left, right);
        return new Expr(left, right, DIV);
    }

    @Override
    public AbstractPart visitMulExpression(ConditionParser.MulExpressionContext ctx) {
        AbstractPart left = visit(ctx.operand(0));
        AbstractPart right = visit(ctx.operand(1));

        validateNumericBin(left, right);
        return new Expr(left, right, MUL);
    }
}
