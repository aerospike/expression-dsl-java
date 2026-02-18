package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void floatToIntComparison() {
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1"));
        // Int is default
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > $.floatBin1.asInt()"), expectedExp);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > $.floatBin1.asInt()"), expectedExp);
    }

    @Test
    void intToFloatComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > $.intBin2.asFloat()"),
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }

    // --- Literal casting tests ---

    @Test
    void intLiteralToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("28.asFloat() == 28.0"),
                Exp.eq(Exp.toFloat(Exp.val(28)), Exp.val(28.0)));
    }

    @Test
    void floatLiteralToInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("27.0.asInt() == 27"),
                Exp.eq(Exp.toInt(Exp.val(27.0)), Exp.val(27)));
    }

    @Test
    void negativeIntToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-5.asFloat() == -5.0"),
                Exp.eq(Exp.toFloat(Exp.val(-5)), Exp.val(-5.0)));
    }

    @Test
    void negativeFloatToInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-5.5.asInt() == -5"),
                Exp.eq(Exp.toInt(Exp.val(-5.5)), Exp.val(-5)));
    }

    @Test
    void zeroIntToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0.asFloat() == 0.0"),
                Exp.eq(Exp.toFloat(Exp.val(0)), Exp.val(0.0)));
    }

    @Test
    void zeroFloatToInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0.0.asInt() == 0"),
                Exp.eq(Exp.toInt(Exp.val(0.0)), Exp.val(0)));
    }

    @Test
    void leadingDotFloatToInt() {
        // Leading-dot float literal (.37) with cast - tests grammar ambiguity
        // between floatOperand ('.' INT) and operandCast (numberOperand '.' pathFunctionCast)
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of(".37.asInt() == 0"),
                Exp.eq(Exp.toInt(Exp.val(0.37)), Exp.val(0)));
    }
}
