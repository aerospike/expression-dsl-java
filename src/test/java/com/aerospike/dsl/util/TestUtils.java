package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.DSLParserImpl;
import lombok.experimental.UtilityClass;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    private final DSLParserImpl parser = new DSLParserImpl();

    public static void parseExpression(String input) {
        parser.parseExpression(input);
    }

    public static void parseExpressionAndCompare(String input, Exp expected) {
        Expression actualExpression = parser.parseExpression(input);
        Expression expectedExpression = Exp.build(expected);
        assertEquals(actualExpression, expectedExpression);
    }

    public static void parseFilters(String input) {
        parser.parseFilters(input);
    }

    public static void parseFiltersAndCompare(String input, List<Filter> expected) {
        List<Filter> actualFilter = parser.parseFilters(input);
        assertEquals(actualFilter, expected);
    }
}
