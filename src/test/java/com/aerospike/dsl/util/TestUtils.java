package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.DSLParserImpl;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.ParsedExpression;
import lombok.experimental.UtilityClass;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    private final DSLParserImpl parser = new DSLParserImpl();
    
    public static Expression parseExp(String input) {
        return Exp.build(parser.parseExpression(input).getResult().getExp());
    }

    public static void parseExpAndCompare(String input, Exp expected) {
        Expression actualExpression = Exp.build(parser.parseExpression(input).getResult().getExp());
        Expression expectedExpression = Exp.build(expected);
        assertEquals(actualExpression, expectedExpression);
    }

    public static Filter parseFilter(String input) {
        return parser.parseExpression(input).getResult().getFilter();
    }

    public static Filter parseFilter(String input, IndexContext indexContext) {
        return parser.parseExpression(input, indexContext).getResult().getFilter();
    }

    public static void parseFilterAndCompare(String input, Filter expected) {
        Filter actualFilter = parseFilter(input);
        assertEquals(actualFilter, expected);
    }

    public static void parseFilterAndCompare(String input, IndexContext indexContext, Filter expected) {
        Filter actualFilter = parseFilter(input, indexContext);
        assertEquals(actualFilter, expected);
    }

    public static void parseExpressionAndCompare(String input, Filter filter, Exp exp) {
        ParsedExpression actualExpression = parser.parseExpression(input);
        assertEquals(actualExpression.getResult().getFilter(), filter);
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(actualExp == null ? null : Exp.build(actualExp), exp == null ? null : Exp.build(exp));
    }

    public static void parseExpressionAndCompare(String input, Filter filter, Exp exp, IndexContext indexContext) {
        ParsedExpression actualExpression = parser.parseExpression(input, indexContext);
        assertEquals(actualExpression.getResult().getFilter(), filter);
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(actualExp == null ? null : Exp.build(actualExp), exp == null ? null : Exp.build(exp));
    }
}
