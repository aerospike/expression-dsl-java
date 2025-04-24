package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.DSLParserImpl;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.index.Index;
import lombok.experimental.UtilityClass;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    private final DSLParserImpl parser = new DSLParserImpl();

    public static Expression parseFilterExp(String input) {
        return parser.parseFilterExpression(input);
    }

    public static void parseFilterExpAndCompare(String input, Exp expected) {
        Expression actualExpression = parser.parseFilterExpression(input);
        Expression expectedExpression = Exp.build(expected);
        assertEquals(actualExpression, expectedExpression);
    }

    public static Filter parseFilter(String input) {
        return parser.parseFilter(input, null, null);
    }

    public static void parseFilterAndCompare(String input, Filter expected) {
        Filter actualFilter = parser.parseFilter(input, null, null);
        assertEquals(actualFilter, expected);
    }

    public static void parseFilterAndCompare(String input, String namespace, List<Index> indexes, Filter expected) {
        Filter actualFilter = parser.parseFilter(input, namespace, indexes);
        assertEquals(actualFilter, expected);
    }

    public static void parseExpressionAndCompare(String input, ParsedExpression expected) {
        ParsedExpression actualExpression = parser.parseDslExpression(input, null, null);
        assertEquals(actualExpression.getFilterExpression(), expected.getFilterExpression());
        assertEquals(actualExpression.getSIFilter(), expected.getSIFilter());
    }

    public static void parseExpressionAndCompare(String input, String namespace, List<Index> indexes,
                                                 ParsedExpression expected) {
        ParsedExpression actualExpression = parser.parseDslExpression(input, namespace, indexes);
        assertEquals(actualExpression.getFilterExpression(), expected.getFilterExpression());
        assertEquals(actualExpression.getSIFilter(), expected.getSIFilter());
    }
}
