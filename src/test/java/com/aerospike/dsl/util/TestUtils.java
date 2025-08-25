package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.InputContext;
import com.aerospike.dsl.ParseResult;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.impl.DSLParserImpl;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    public static final String NAMESPACE = "test1";
    private static final DSLParserImpl parser = new DSLParserImpl();

    /**
     * Parses the given DSL expression and extracts the resulting {@link Exp} object.
     *
     * @param inputContext The input representing DSL expression
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static Exp parseFilterExp(InputContext inputContext) {
        return parser.parseExpression(inputContext).getResult().getExp();
    }

    /**
     * Parses the given DSL expression and returns the resulting {@link ParsedExpression} object.
     *
     * @param inputContext The {@link InputContext} representing DSL expression
     * @param indexContext The {@link IndexContext} to be used for building secondary index filter
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static ParsedExpression getParsedExpression(InputContext inputContext, IndexContext indexContext) {
        return parser.parseExpression(inputContext, indexContext);
    }

    /**
     * Parses the given DSL expression and returns the resulting {@link ParsedExpression} object.
     *
     * @param exprTree          The {@link AbstractPart} representing parsed expression tree, as returned by {@link ParsedExpression#getExprTree()}
     * @param placeholderValues The {@link PlaceholderValues} to be matched with placeholders by indexes
     * @param indexesMap        The map of indexes by namespaces, as returned by {@link ParsedExpression#getIndexesMap()}
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static ParseResult getParseResult(AbstractPart exprTree, PlaceholderValues placeholderValues,
                                             Map<String, List<Index>> indexesMap) {
        return ParsedExpression.getResult(exprTree, placeholderValues, indexesMap);
    }

    /**
     * Parses the given DSL expression, extracts the resulting {@link Exp} object, converts it to an {@link Expression}
     * object, and then asserts that it is equal to the {@code expected} {@link Exp} also built into an
     * {@link Expression}.
     *
     * @param inputContext The input representing DSL expression
     * @param expected     The expected {@link Exp} object to compare against the parsed result
     */
    public static void parseFilterExpressionAndCompare(InputContext inputContext, Exp expected) {
        Expression actualExpression = Exp.build(parser.parseExpression(inputContext).getResult().getExp());
        Expression expectedExpression = Exp.build(expected);
        assertEquals(expectedExpression, actualExpression);
    }

    /**
     * Parses the given DL expression using the provided {@link InputContext} to match placeholders
     * and returns the resulting {@link Filter} object.
     *
     * @param inputContext The {@link InputContext} to be used to match placeholders
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(InputContext inputContext) {
        return parser.parseExpression(inputContext).getResult().getFilter();
    }

    /**
     * Parses the given DL expression using the provided {@link IndexContext} and returns the resulting {@link Filter} object.
     *
     * @param inputContext The input representing DSL expression
     * @param indexContext The {@link IndexContext} to be used for building secondary index filter
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(InputContext inputContext, IndexContext indexContext) {
        return parser.parseExpression(inputContext, indexContext).getResult().getFilter();
    }

    /**
     * Parses the given DSL expression and asserts that the result is equal to the {@code expected} {@link Filter}
     * object.
     *
     * @param input    The input representing DSL expression
     * @param expected The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(InputContext input, Filter expected) {
        Filter actualFilter = parseFilter(input);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given DSL expression using the provided {@link IndexContext} and asserts that the result is equal to
     * the {@code expected} {@link Filter} object.
     *
     * @param input        The string input representing DSL expression
     * @param indexContext The {@link IndexContext} to be used for building secondary index filter
     * @param expected     The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(InputContext input, IndexContext indexContext, Filter expected) {
        Filter actualFilter = parseFilter(input, indexContext);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given DSL expression and compares the resulting
     * {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param inputContext The input representing DSL expression
     * @param filter       The expected {@link Filter} component of the parsed result
     * @param exp          The expected {@link Exp} component of the parsed result. Can be {@code null}
     */
    public static void parseDslExpressionAndCompare(InputContext inputContext, Filter filter, Exp exp) {
        ParsedExpression actualExpression = parser.parseExpression(inputContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }

    /**
     * Parses the given DSL expression using the provided {@link IndexContext}
     * and compares the resulting {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param inputContext The input representing DSL expression
     * @param filter       The expected {@link Filter} component of the parsed result
     * @param exp          The expected {@link Exp} component of the parsed result. Can be {@code null}
     * @param indexContext The {@link IndexContext} to be used for building secondary index filter
     */
    public static void parseDslExpressionAndCompare(InputContext inputContext, Filter filter, Exp exp, IndexContext indexContext) {
        ParsedExpression actualExpression = parser.parseExpression(inputContext, indexContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }
}
