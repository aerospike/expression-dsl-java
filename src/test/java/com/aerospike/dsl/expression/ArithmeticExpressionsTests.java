package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArithmeticExpressionsTests {

    @Test
    void add() {
        TestUtils.parseFilterExpressionAndCompare("($.apples + $.bananas) > 10",
                Exp.gt(Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples + 5) > 10",
                Exp.gt(Exp.add(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(5 + $.bananas) > 10",
                Exp.gt(Exp.add(Exp.val(5), Exp.intBin("bananas")), Exp.val(10)));

        TestUtils.parseFilterExpressionAndCompare("(5.2 + $.bananas) > 10.2",
                Exp.gt(Exp.add(Exp.val(5.2), Exp.floatBin("bananas")), Exp.val(10.2)));
    }

    @Test
    void sub() {
        TestUtils.parseFilterExpressionAndCompare("($.apples - $.bananas) == 10",
                Exp.eq(Exp.sub(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples - 5) == 10",
                Exp.eq(Exp.sub(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(15 - $.bananas) == 10",
                Exp.eq(Exp.sub(Exp.val(15), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void mul() {
        TestUtils.parseFilterExpressionAndCompare("($.apples * $.bananas) != 10",
                Exp.ne(Exp.mul(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples * 7) != 10",
                Exp.ne(Exp.mul(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(3 * $.bananas) != 10",
                Exp.ne(Exp.mul(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void div() {
        TestUtils.parseFilterExpressionAndCompare("($.apples / $.bananas) <= 10",
                Exp.le(Exp.div(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples / 5) <= 10",
                Exp.le(Exp.div(Exp.intBin("apples"), Exp.val(5)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(33 / $.bananas) <= 10",
                Exp.le(Exp.div(Exp.val(33), Exp.intBin("bananas")), Exp.val(10)));
        // Exp should be constructed and equal and therefore the test is passing
        // but when actually triggered server will throw divide by zero exception
        TestUtils.parseFilterExpressionAndCompare("($.apples / 0) <= 10",
                Exp.le(Exp.div(Exp.intBin("apples"), Exp.val(0)), Exp.val(10)));
    }

    @Test
    void mod() {
        TestUtils.parseFilterExpressionAndCompare("($.apples % $.bananas) != 10",
                Exp.ne(Exp.mod(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples % 7) != 10",
                Exp.ne(Exp.mod(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(3 % $.bananas) != 10",
                Exp.ne(Exp.mod(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intAnd() {
        TestUtils.parseFilterExpressionAndCompare("($.apples & $.bananas) != 10",
                Exp.ne(Exp.intAnd(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples & 7) != 10",
                Exp.ne(Exp.intAnd(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(3 & $.bananas) != 10",
                Exp.ne(Exp.intAnd(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intOr() {
        TestUtils.parseFilterExpressionAndCompare("($.apples | $.bananas) != 10",
                Exp.ne(Exp.intOr(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples | 7) != 10",
                Exp.ne(Exp.intOr(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(3 | $.bananas) != 10",
                Exp.ne(Exp.intOr(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intXor() {
        TestUtils.parseFilterExpressionAndCompare("($.apples ^ $.bananas) != 10",
                Exp.ne(Exp.intXor(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("($.apples ^ 7) != 10",
                Exp.ne(Exp.intXor(Exp.intBin("apples"), Exp.val(7)), Exp.val(10)));
        TestUtils.parseFilterExpressionAndCompare("(3 ^ $.bananas) != 10",
                Exp.ne(Exp.intXor(Exp.val(3), Exp.intBin("bananas")), Exp.val(10)));
    }

    @Test
    void intNot() {
        TestUtils.parseFilterExpressionAndCompare("(~$.apples) != 10",
                Exp.ne(Exp.intNot(Exp.intBin("apples")), Exp.val(10)));
    }

    @Test
    void intLShift() {
        TestUtils.parseFilterExpressionAndCompare("$.visits << 1",
                Exp.lshift(Exp.intBin("visits"), Exp.val(1)));
    }

    @Test
    void intRShift() {
        TestUtils.parseFilterExpressionAndCompare("(($.flags >> 6) & 1) == 1",
                Exp.eq(Exp.intAnd(Exp.rshift(Exp.intBin("flags"), Exp.val(6)), Exp.val(1)), Exp.val(1)));
    }

    @Test
    void negativeArithmetic() {
        assertThatThrownBy(() -> parseFilterExp("($.apples.get(type: STRING) + 5) > 10"))
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
