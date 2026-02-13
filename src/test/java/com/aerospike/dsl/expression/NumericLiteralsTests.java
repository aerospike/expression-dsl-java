package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Comprehensive tests for numeric literal parsing: INT (decimal, hex, binary) and FLOAT.
 * <p>
 * INT spec: optional sign (+/-), hex (0x/0X), binary (0b/0B), decimal digits.
 * FLOAT spec: optional sign (+/-), digits, exactly one decimal separator (not the last char).
 * Leading-dot floats (.37) are a documented limitation -- not supported.
 */
public class NumericLiteralsTests {

    // ==================== INT: decimal ====================

    @Test
    void intDecimalPositive() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("5 == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void intDecimalNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-5 == -5"),
                Exp.eq(Exp.val(-5), Exp.val(-5)));
    }

    @Test
    void intDecimalPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+5 == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void intDecimalZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0 == 0"),
                Exp.eq(Exp.val(0), Exp.val(0)));
    }

    // ==================== INT: hexadecimal ====================

    @Test
    void intHexLowercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0xff == 255"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexUppercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0XFF == 255"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-0xff == -255"),
                Exp.eq(Exp.val(-255), Exp.val(-255)));
    }

    @Test
    void intHexPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+0xff == 255"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0x0 == 0"),
                Exp.eq(Exp.val(0), Exp.val(0)));
    }

    @Test
    void intHexCaseInsensitive() {
        // Mixed-case hex digits should parse identically
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0xff == 0xFF"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    // ==================== INT: binary ====================

    @Test
    void intBinaryLowercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0b1010 == 10"),
                Exp.eq(Exp.val(10), Exp.val(10)));
    }

    @Test
    void intBinaryUppercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0B1010 == 10"),
                Exp.eq(Exp.val(10), Exp.val(10)));
    }

    @Test
    void intBinaryNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-0b1010 == -10"),
                Exp.eq(Exp.val(-10), Exp.val(-10)));
    }

    @Test
    void intBinaryPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+0b1010 == 10"),
                Exp.eq(Exp.val(10), Exp.val(10)));
    }

    @Test
    void intBinaryZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0b0 == 0"),
                Exp.eq(Exp.val(0), Exp.val(0)));
    }

    // ==================== FLOAT ====================

    @Test
    void floatStandard() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("3.14 == 3.14"),
                Exp.eq(Exp.val(3.14), Exp.val(3.14)));
    }

    @Test
    void floatNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-34.1 == -34.1"),
                Exp.eq(Exp.val(-34.1), Exp.val(-34.1)));
    }

    @Test
    void floatPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+3.14 == 3.14"),
                Exp.eq(Exp.val(3.14), Exp.val(3.14)));
    }

    @Test
    void floatZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0.0 == 0.0"),
                Exp.eq(Exp.val(0.0), Exp.val(0.0)));
    }

    @Test
    void floatPositiveZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+0.0 == 0.0"),
                Exp.eq(Exp.val(0.0), Exp.val(0.0)));
    }

    // ==================== Sign behavior with operators ====================

    @Test
    void plusAsOperatorWithSpaces() {
        // Verify that '+' with spaces is treated as an addition operator, not a sign
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5 + 3) == 8"),
                Exp.eq(Exp.add(Exp.val(5), Exp.val(3)), Exp.val(8)));
    }

    @Test
    void minusAsOperatorWithSpaces() {
        // Verify that '-' with spaces is treated as a subtraction operator, not a sign
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5 - 3) == 2"),
                Exp.eq(Exp.sub(Exp.val(5), Exp.val(3)), Exp.val(2)));
    }

    // ==================== Negative / error tests ====================

    @Test
    void invalidHexDigits() {
        // 0xGG is not valid hex -- lexer produces INT(0) + NAME_IDENTIFIER(xGG)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0xGG == 0")))
                .isInstanceOf(DslParseException.class);
    }

    @Test
    void invalidBinaryDigits() {
        // 0b2 is not valid binary -- lexer produces INT(0) + NAME_IDENTIFIER(b2)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0b2 == 0")))
                .isInstanceOf(DslParseException.class);
    }

    @Test
    void trailingDotFloat() {
        // "10." is not valid FLOAT -- requires digits after dot
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("10. == 10.0")))
                .isInstanceOf(DslParseException.class);
    }

    @Test
    void leadingDotFloat() {
        // Leading-dot floats (.37) are a documented limitation.
        // The '.' is consumed as a dot token, not as part of FLOAT.
        // ANTLR's error recovery silently drops it, parsing ".37" as INT(37).
        // Users must write "0.37" instead of ".37".
        assertDoesNotThrow(() -> parseFilterExp(ExpressionContext.of(".37 == 0.37")));
    }
}
