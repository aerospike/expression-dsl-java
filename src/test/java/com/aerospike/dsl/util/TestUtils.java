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

    /**
     * Parses the given DSL expression and returns a {@link ParsedExpression} object.
     *
     * @param input The string input representing DSL expression
     * @param indexContext The {@link IndexContext} to be used during parsing
     * @return A {@link ParsedExpression} object containing the result of the parsing
     */
    public static ParsedExpression parseExpression(String input, IndexContext indexContext) {
        return parser.parseExpression(input, indexContext);
    }

    /**
     * Parses the given DSL expression and extracts the resulting {@link Exp} object.
     *
     * @param input The string input representing DSL expression
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static Exp parseFilterExp(String input) {
        return parser.parseExpression(input).getResult().getExp();
    }

    /**
     * Parses the given DSL expression and builds an {@link Expression} object from the resulting
     * {@link Exp}.
     *
     * @param input The string input representing DSL expression
     * @return An {@link Expression} object built from the parsed filter expression
     */
    public static Expression parseFilterExpression(String input) {
        return Exp.build(parser.parseExpression(input).getResult().getExp());
    }

    /**
     * Parses the given DSL expression, extracts the resulting {@link Exp} object, converts it to an {@link Expression} object,
     * and then asserts that it is equal to the {@code expected} {@link Exp} also built into an {@link Expression}.
     *
     * @param input The string input representing DSL expression
     * @param expected The expected {@link Exp} object to compare against the parsed result
     */
    public static void parseFilterExpressionAndCompare(String input, Exp expected) {
        Expression actualExpression = Exp.build(parser.parseExpression(input).getResult().getExp());
        Expression expectedExpression = Exp.build(expected);
        assertEquals(expectedExpression, actualExpression);
    }

    /**
     * Parses the given DSL expression and returns the resulting {@link Filter} object.
     * This method uses the parser without an {@link IndexContext}.
     *
     * @param input The string input representing DSL expression
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(String input) {
        return parser.parseExpression(input).getResult().getFilter();
    }

    /**
     * Parses the given DL expression using the provided {@link IndexContext} and returns the resulting {@link Filter} object.
     *
     * @param input The string input representing DSL expression
     * @param indexContext The {@link IndexContext} to be used during parsing
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(String input, IndexContext indexContext) {
        return parser.parseExpression(input, indexContext).getResult().getFilter();
    }

    /**
     * Parses the given DSL expression and asserts that the result is equal to the {@code expected} {@link Filter} object.
     *
     * @param input The string input representing DSL expression
     * @param expected The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(String input, Filter expected) {
        Filter actualFilter = parseFilter(input);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given DSL expression using the provided {@link IndexContext} and asserts that the result is equal to the {@code expected} {@link Filter} object.
     *
     * @param input The string input representing DSL expression
     * @param indexContext The {@link IndexContext} to be used during parsing
     * @param expected The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(String input, IndexContext indexContext, Filter expected) {
        Filter actualFilter = parseFilter(input, indexContext);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given DSL expression and compares the resulting
     * {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param input The string input representing DSL expression
     * @param filter The expected {@link Filter} component of the parsed result
     * @param exp The expected {@link Exp} component of the parsed result. Can be {@code null}
     */
    public static void parseDslExpressionAndCompare(String input, Filter filter, Exp exp) {
        ParsedExpression actualExpression = parser.parseExpression(input);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }

    /**
     * Parses the given DSL expression using the provided {@link IndexContext}
     * and compares the resulting {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param input The string input representing DSL expression
     * @param filter The expected {@link Filter} component of the parsed result
     * @param exp The expected {@link Exp} component of the parsed result. Can be {@code null}
     * @param indexContext The {@link IndexContext} to be used during parsing
     */
    public static void parseDslExpressionAndCompare(String input, Filter filter, Exp exp, IndexContext indexContext) {
        ParsedExpression actualExpression = parser.parseExpression(input, indexContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }
}
