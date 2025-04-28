package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.DSLParserImpl;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.ParsedExpression;
import lombok.experimental.UtilityClass;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    private final DSLParserImpl parser = new DSLParserImpl();
    public static Expression parseExp(String input) {
        return Exp.build(parser.parseExpression(input).getResultPair().getExp());
    }

    public static void parseExpAndCompare(String input, Exp expected) {
        Expression actualExpression = Exp.build(parser.parseExpression(input).getResultPair().getExp());
        Expression expectedExpression = Exp.build(expected);
        assertEquals(actualExpression, expectedExpression);
    }

    public static Filter parseFilter(String input) {
        return parser.parseExpression(input).getResultPair().getFilter();
    }

    public static Filter parseFilter(String input, String namespace, List<Index> indexes) {
        return parser.parseExpression(input, namespace, indexes).getResultPair().getFilter();
    }

    public static void parseFilterAndCompare(String input, Filter expected) {
        Filter actualFilter = parseFilter(input);
        assertEquals(actualFilter, expected);
    }

    public static void parseFilterAndCompare(String input, String namespace, List<Index> indexes, Filter expected) {
        Filter actualFilter = parseFilter(input, namespace, indexes);
        assertEquals(actualFilter, expected);
    }

    public static void parseExpressionAndCompare(String input, Filter filter, Exp exp) {
        ParsedExpression actualExpression = parser.parseExpression(input);
        assertEquals(actualExpression.getResultPair().getFilter(), filter);
        Exp actualExp = actualExpression.getResultPair().getExp();
        assertEquals(actualExp == null ? null : Exp.build(actualExp), exp == null ? null : Exp.build(exp));
    }

    public static void parseExpressionAndCompare(String input, Filter filter, Exp exp,
                                                 String namespace, List<Index> indexes) {
        ParsedExpression actualExpression = parser.parseExpression(input, namespace, indexes);
        assertEquals(actualExpression.getResultPair().getFilter(), filter);
        Exp actualExp = actualExpression.getResultPair().getExp();
        assertEquals(actualExp == null ? null : Exp.build(actualExp), exp == null ? null : Exp.build(exp));
    }
}
