package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArithmeticExpressionsTests {

    @Test
    void add() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples + $.bananas) > 10"),
                Exp.gt(Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples + 5) > 10"),
                Exp.gt(Exp.add(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5 + $.bananas) > 10"),
                Exp.gt(Exp.add(Exp.val(5), Exp.intBin("bananas")), Exp.val(10)));

        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5.2 + $.bananas) > 10.2"),
                Exp.gt(Exp.add(Exp.val(5.2), Exp.floatBin("bananas")), Exp.val(10.2)));
    }

    @Test
    void sub() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples - $.bananas) == 10"),
                Exp.eq(Exp.sub(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples - 5) == 10"),
                Exp.eq(Exp.sub(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(15 - $.bananas) == 10"),
                Exp.eq(Exp.sub(Exp.val(15), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void mul() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples * $.bananas) != 10"),
                Exp.ne(Exp.mul(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples * 7) != 10"),
                Exp.ne(Exp.mul(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(3 * $.bananas) != 10"),
                Exp.ne(Exp.mul(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void div() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples / $.bananas) <= 10"),
                Exp.le(Exp.div(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples / 5) <= 10"),
                Exp.le(Exp.div(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(33 / $.bananas) <= 10"),
                Exp.le(Exp.div(Exp.val(33), Exp.intBin("bananas")), Exp.val(10)));
        // Exp should be constructed and equal and therefore the test is passing
        // but when actually triggered server will throw divide by zero exception
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples / 0) <= 10"),
                Exp.le(Exp.div(Exp.intBin("apples"), Exp.val(0)), Exp.val(10)));
    }

    @Test
    void mod() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples % $.bananas) != 10"),
                Exp.ne(Exp.mod(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples % 7) != 10"),
                Exp.ne(Exp.mod(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(3 % $.bananas) != 10"),
                Exp.ne(Exp.mod(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void pow() {
        // Basic power with bin
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.base ** 2.0) == 4.0"),
                Exp.eq(Exp.pow(Exp.floatBin("base"), Exp.val(2.0)), Exp.val(4.0)));
    }

    @Test
    void powRightAssociativity() {
        // 4.0 ** 3.0 ** 2.0 must parse as 4.0 ** (3.0 ** 2.0), NOT (4.0 ** 3.0) ** 2.0
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("(4.0 ** 3.0 ** 2.0) == 0.0"),
                Exp.eq(
                        Exp.pow(Exp.val(4.0), Exp.pow(Exp.val(3.0), Exp.val(2.0))),
                        Exp.val(0.0)));
    }

    @Test
    void powPrecedenceWithMultiplication() {
        // 2.0 * 3.0 ** 2.0 must parse as 2.0 * (3.0 ** 2.0) since ** binds tighter than *
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("(2.0 * 3.0 ** 2.0) == 18.0"),
                Exp.eq(
                        Exp.mul(Exp.val(2.0), Exp.pow(Exp.val(3.0), Exp.val(2.0))),
                        Exp.val(18.0)));
    }

    @Test
    void intAnd() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples & $.bananas) != 10"),
                Exp.ne(Exp.intAnd(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples & 7) != 10"),
                Exp.ne(Exp.intAnd(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(3 & $.bananas) != 10"),
                Exp.ne(Exp.intAnd(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intOr() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples | $.bananas) != 10"),
                Exp.ne(Exp.intOr(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples | 7) != 10"),
                Exp.ne(Exp.intOr(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(3 | $.bananas) != 10"),
                Exp.ne(Exp.intOr(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intXor() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples ^ $.bananas) != 10"),
                Exp.ne(Exp.intXor(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples ^ 7) != 10"),
                Exp.ne(Exp.intXor(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(3 ^ $.bananas) != 10"),
                Exp.ne(Exp.intXor(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intNot() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(~$.apples) != 10"),
                Exp.ne(Exp.intNot(Exp.intBin("apples")), Exp.val(10)));
    }

    @Test
    void intLShift() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.visits << 1"),
                Exp.lshift(Exp.intBin("visits"), Exp.val(1)));
    }

    @Test
    void intARShift() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(($.flags >> 6) & 1) == 1"),
                Exp.eq(Exp.intAnd(Exp.arshift(Exp.intBin("flags"), Exp.val(6)), Exp.val(1)), Exp.val(1)));
    }

    @Test
    void intLogicalRShift() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.flags >>> 2) == 4"),
                Exp.eq(Exp.rshift(Exp.intBin("flags"), Exp.val(2)), Exp.val(4)));
    }

    @Test
    void arithmeticRShiftNegativeValue() {
        // Arithmetic right shift preserves sign bit: (-1) >> 1 == -1
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("((-1) >> 1) == -1"),
                Exp.eq(Exp.arshift(Exp.val(-1), Exp.val(1)), Exp.val(-1)));
    }

    @Test
    void logicalRShiftNegativeValue() {
        // Logical right shift fills with zeros: (-1) >>> 1 produces a large positive number
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("((-1) >>> 1) > 0"),
                Exp.gt(Exp.rshift(Exp.val(-1), Exp.val(1)), Exp.val(0)));
    }

    @Test
    void intARShiftHex() {
        // Spec example: 0x20 >> 1 == 16
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(0x20 >> 1) == 16"),
                Exp.eq(Exp.arshift(Exp.val(32), Exp.val(1)), Exp.val(16)));
    }

    @Test
    void intLogicalRShiftBinary() {
        // Spec example: 0b10000 >>> 2 == 4
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(0b10000 >>> 2) == 4"),
                Exp.eq(Exp.rshift(Exp.val(16), Exp.val(2)), Exp.val(4)));
    }

    @Test
    void intLShiftHex() {
        // 0x1 << 4 == 16
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(0x1 << 4) == 0x10"),
                Exp.eq(Exp.lshift(Exp.val(1), Exp.val(4)), Exp.val(16)));
    }

    @Test
    void intLShiftBinary() {
        // 0b1 << 3 == 8
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(0b1 << 3) == 0b1000"),
                Exp.eq(Exp.lshift(Exp.val(1), Exp.val(3)), Exp.val(8)));
    }

    @Test
    void hexCaseInsensitive() {
        // 0xff and 0xFF should both be 255
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(0xff >> 1) == (0xFF >> 1)"),
                Exp.eq(Exp.arshift(Exp.val(255), Exp.val(1)), Exp.arshift(Exp.val(255), Exp.val(1))));
    }

    @Test
    void shiftByZeroIsIdentity() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.a >> 0) == $.a"),
                Exp.eq(Exp.arshift(Exp.intBin("a"), Exp.val(0)), Exp.intBin("a")));
    }

    // --- Spaceless arithmetic (no whitespace around operators) ---

    @Test
    void addSpacelessBinAndLiteral() {
        // '$.apples+5' must parse '+' as operator, not sign prefix on '5'
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples+5) > 10"),
                Exp.gt(Exp.add(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
    }

    @Test
    void subSpacelessBinAndLiteral() {
        // '$.apples-5' must parse '-' as operator, not sign prefix on '5'
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples-5) == 10"),
                Exp.eq(Exp.sub(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
    }

    @Test
    void addSpacelessLiteralAndBin() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5+$.bananas) > 10"),
                Exp.gt(Exp.add(Exp.val(5), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void subSpacelessLiteralAndBin() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(15-$.bananas) == 10"),
                Exp.eq(Exp.sub(Exp.val(15), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void spacelessFloatArithmetic() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5.2+$.bananas) > 10.2"),
                Exp.gt(Exp.add(Exp.val(5.2), Exp.floatBin("bananas")), Exp.val(10.2)));
    }

    // --- Function-style arithmetic operations ---

    @Test
    void abs() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("abs($.value) == 12"),
                Exp.eq(Exp.abs(Exp.intBin("value")), Exp.val(12)));
    }

    @Test
    void absNegativeLiteral() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("abs(-42) == 42"),
                Exp.eq(Exp.abs(Exp.val(-42)), Exp.val(42)));
    }

    @Test
    void absZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("abs(0) == 0"),
                Exp.eq(Exp.abs(Exp.val(0)), Exp.val(0)));
    }

    @Test
    void ceil() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("ceil($.price) == 13.0"),
                Exp.eq(Exp.ceil(Exp.floatBin("price")), Exp.val(13.0)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("ceil(12.34) == 13.0"),
                Exp.eq(Exp.ceil(Exp.val(12.34)), Exp.val(13.0)));
    }

    @Test
    void ceilAlreadyWholeNumber() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("ceil(5.0) == 5.0"),
                Exp.eq(Exp.ceil(Exp.val(5.0)), Exp.val(5.0)));
    }

    @Test
    void ceilNegativeFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("ceil(-2.7) == -2.0"),
                Exp.eq(Exp.ceil(Exp.val(-2.7)), Exp.val(-2.0)));
    }

    @Test
    void floor() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("floor($.price) == 12.0"),
                Exp.eq(Exp.floor(Exp.floatBin("price")), Exp.val(12.0)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("floor(12.34) == 12.0"),
                Exp.eq(Exp.floor(Exp.val(12.34)), Exp.val(12.0)));
    }

    @Test
    void floorAlreadyWholeNumber() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("floor(5.0) == 5.0"),
                Exp.eq(Exp.floor(Exp.val(5.0)), Exp.val(5.0)));
    }

    @Test
    void floorNegativeFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("floor(-2.7) == -3.0"),
                Exp.eq(Exp.floor(Exp.val(-2.7)), Exp.val(-3.0)));
    }

    @Test
    void log() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("log($.val, 2.0) == 5.0"),
                Exp.eq(Exp.log(Exp.floatBin("val"), Exp.val(2.0)), Exp.val(5.0)));
    }

    @Test
    void logBaseCase() {
        // log(1.0, 10.0) == 0.0 (log base 10 of 1 is 0)
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("log(1.0, 10.0) == 0.0"),
                Exp.eq(Exp.log(Exp.val(1.0), Exp.val(10.0)), Exp.val(0.0)));
    }

    @Test
    void min() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("min($.a, $.b, $.c) == 3"),
                Exp.eq(Exp.min(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")), Exp.val(3)));
        // With literals
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("min(4, 5, 9, 6, 3) == 3"),
                Exp.eq(Exp.min(Exp.val(4), Exp.val(5), Exp.val(9), Exp.val(6), Exp.val(3)), Exp.val(3)));
    }

    @Test
    void minWithTwoArgs() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("min($.a, $.b) == 3"),
                Exp.eq(Exp.min(Exp.intBin("a"), Exp.intBin("b")), Exp.val(3)));
    }

    @Test
    void max() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("max($.a, $.b, $.c) == 9"),
                Exp.eq(Exp.max(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")), Exp.val(9)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("max(4, 5, 9, 6, 3) == 9"),
                Exp.eq(Exp.max(Exp.val(4), Exp.val(5), Exp.val(9), Exp.val(6), Exp.val(3)), Exp.val(9)));
    }

    @Test
    void maxWithManyArgs() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("max(1, 2, 3, 4, 5, 6) == 6"),
                Exp.eq(Exp.max(Exp.val(1), Exp.val(2), Exp.val(3),
                        Exp.val(4), Exp.val(5), Exp.val(6)), Exp.val(6)));
    }

    // --- Bit functions ---

    @Test
    void countOneBits() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("countOneBits($.flags) == 4"),
                Exp.eq(Exp.count(Exp.intBin("flags")), Exp.val(4)));
    }

    @Test
    void countOneBitsZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("countOneBits(0) == 0"),
                Exp.eq(Exp.count(Exp.val(0)), Exp.val(0)));
    }

    @Test
    void countOneBitsAllOnes() {
        // -1 in two's complement has all 64 bits set
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("countOneBits(-1) == 64"),
                Exp.eq(Exp.count(Exp.val(-1)), Exp.val(64)));
    }

    @Test
    void findBitLeft() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("findBitLeft($.bits, true) == 4"),
                Exp.eq(Exp.lscan(Exp.intBin("bits"), Exp.val(true)), Exp.val(4)));
    }

    @Test
    void findBitRight() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("findBitRight($.bits, false) == 4"),
                Exp.eq(Exp.rscan(Exp.intBin("bits"), Exp.val(false)), Exp.val(4)));
    }

    @Test
    void findBitLeftWithLiterals() {
        // Spec example pattern: findBitLeft(30, true)
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("findBitLeft(30, true) == 59"),
                Exp.eq(Exp.lscan(Exp.val(30), Exp.val(true)), Exp.val(59)));
    }

    @Test
    void findBitRightWithLiterals() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("findBitRight(27, false) == 2"),
                Exp.eq(Exp.rscan(Exp.val(27), Exp.val(false)), Exp.val(2)));
    }

    // --- Nested / compound function tests ---

    @Test
    void nestedFunctionCalls() {
        // abs(ceil(12.34))
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("abs(ceil(12.34)) == 13.0"),
                Exp.eq(Exp.abs(Exp.ceil(Exp.val(12.34))), Exp.val(13.0)));
    }

    @Test
    void functionWithExpressionArg() {
        // abs($.a - $.b) -- function argument is an arithmetic expression
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("abs($.a - $.b) > 0"),
                Exp.gt(Exp.abs(Exp.sub(Exp.intBin("a"), Exp.intBin("b"))), Exp.val(0)));
    }

    @Test
    void functionsInArithmeticExpression() {
        // (abs($.a) + abs($.b)) > 10 -- functions used as operands in arithmetic
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(abs($.a) + abs($.b)) > 10"),
                Exp.gt(Exp.add(Exp.abs(Exp.intBin("a")), Exp.abs(Exp.intBin("b"))), Exp.val(10)));
    }

    @Test
    void functionCombination() {
        // min($.a, $.b) > max($.c, $.d)
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("min($.a, $.b) > max($.c, $.d)"),
                Exp.gt(
                        Exp.min(Exp.intBin("a"), Exp.intBin("b")),
                        Exp.max(Exp.intBin("c"), Exp.intBin("d"))));
    }

    @Test
    void bitwiseInFunctionArg() {
        // countOneBits($.a & $.b) -- bitwise expression inside function
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("countOneBits($.a & $.b) > 3"),
                Exp.gt(Exp.count(Exp.intAnd(Exp.intBin("a"), Exp.intBin("b"))), Exp.val(3)));
    }

    // --- Negative / error tests for functions ---

    @Test
    void negativeUnknownFunction() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("unknown($.a) == 5")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Unknown function");
    }

    @Test
    void negativeWrongArgCountUnary() {
        // abs expects 1 arg, given 2
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("abs($.a, $.b) == 5")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("expects 1 argument(s)");
    }

    @Test
    void negativeWrongArgCountBinary() {
        // log expects 2 args, given 1
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("log(2.0) == 0.0")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("expects 2 argument(s)");
    }

    @Test
    void negativeMinTooFewArgs() {
        // min with 1 arg should fail (needs >= 2)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("min(5) == 5")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("at least 2 arguments");
    }

    @Test
    void negativeArithmetic() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("($.apples.get(type: STRING) + 5) > 10")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare STRING to INT");

        // TODO: should throw an exception (cannot use arithmetic operations on Strings)
        //assertThatThrownBy(() -> translate("$.apples + \"stringVal\""))
        //                .isInstanceOf(AerospikeDSLException.class)
        //        .hasMessageContaining("Cannot compare STRING to INT");

        // TODO: should throw an exception (result of an arithmetic operation is not a String)
        //assertThatThrownBy(() -> translate("($.apples * $.bananas) != \"stringVal\""))
        //        .isInstanceOf(AerospikeDSLException.class)
        //        .hasMessageContaining("Cannot compare STRING to INT");
    }
}
