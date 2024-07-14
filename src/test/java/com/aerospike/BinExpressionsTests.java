package com.aerospike;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.aerospike.TestUtils.translateAndCompare;
import static com.aerospike.TestUtils.translateAndPrint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BinExpressionsTests {

    @Test
    void binGT() {
        // Bin as right
        translateAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        translateAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

        // Bin as left (considered as lt)
        translateAndCompare("100 < $.intBin1", Exp.lt(Exp.val(100), Exp.intBin("intBin1")));
        translateAndCompare("'text' < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
        translateAndCompare("\"text\" < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));

        // 2 Bins (Integer)
        translateAndCompare("$.intBin1 < $.intBin2", Exp.lt(Exp.intBin("intBin1"), Exp.intBin("intBin2")));
    }

    @Test
    void binGE() {
        translateAndCompare("$.intBin1 >= 100", Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.stringBin1 >= 'text'", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        translateAndCompare("$.stringBin1 >= \"text\"", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));

        // Float
        translateAndCompare("$.floatBin1 >= 100.25", Exp.ge(Exp.floatBin("floatBin1"), Exp.val(100.25)));
    }

    @Test
    void binLT() {
        translateAndCompare("$.intBin1 < 100", Exp.lt(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.stringBin1 < 'text'", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
        translateAndCompare("$.stringBin1 < \"text\"", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLE() {
        translateAndCompare("$.intBin1 <= 100", Exp.le(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.stringBin1 <= 'text'", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
        translateAndCompare("$.stringBin1 <= \"text\"", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binEquals() {
        translateAndCompare("$.intBin1 == 100", Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.strBin == \"yes\"", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        translateAndCompare("$.strBin == 'yes'", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));

        // Boolean
        translateAndCompare("$.boolBin1 == true", Exp.eq(Exp.boolBin("boolBin1"), Exp.val(true)));
        translateAndCompare("false == $.boolBin1", Exp.eq(Exp.val(false), Exp.boolBin("boolBin1")));

        /*
        TODO: This currently fails, will be supported on FMWK-475 Explicit Type Comparison

        translateAndCompare("$.stringBin1 == $.stringBin2",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.stringBin("stringBin2")));

         */
    }

    @Test
    void binNotEquals() {
        translateAndCompare("$.intBin1 != 100", Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.strBin != \"yes\"", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        translateAndCompare("$.strBin != 'yes'", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));

        // Boolean
        translateAndCompare("$.boolBin1 != false", Exp.ne(Exp.boolBin("boolBin1"), Exp.val(false)));
    }

    @Test
    void stringBinEqualsNegativeTest() {
        assertThatThrownBy(() -> translateAndPrint("$.strBin == yes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unable to parse right operand");
    }

    @Test
    void binLogicalAndOrCombinations() {
        Exp testExp1 = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100", testExp1);

        Exp testExp2 = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
        );
        // TODO: what should be the default behaviour with no parentheses?
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100", testExp2);
        translateAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100", testExp2);

        Exp testExp3 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
                )
        );
        translateAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", testExp3);

        // check that parentheses make difference
        assertThatThrownBy(() -> translateAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", testExp2))
                .isInstanceOf(AssertionFailedError.class);
    }

    @Test
    void binBooleanImplicitLogicalComparison() {
        translateAndCompare("$.boolBin1 and $.boolBin2",
                Exp.and(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
        translateAndCompare("$.boolBin1 or $.boolBin2",
                Exp.or(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
        translateAndCompare("not($.boolBin1)",
                Exp.not(Exp.boolBin("boolBin1")));
        translateAndCompare("exclusive($.boolBin1, $.boolBin2)",
                Exp.exclusive(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
    }

    @Test
    void binLogicalNot() {
        translateAndCompare("not($.keyExists())", Exp.not(Exp.keyExists()));
    }

    @Test
    void binLogicalExclusive() {
        translateAndCompare("exclusive($.hand == \"hook\", $.leg == \"peg\")",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")),
                        Exp.eq(Exp.stringBin("leg"), Exp.val("peg"))));
    }

    @Test
    void negativeBinLogicalExclusiveWithOneParam() {
        assertThatThrownBy(() -> translateAndCompare("exclusive($.hand == \"hook\")",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")))))
                .isInstanceOf(NullPointerException.class);
    }

    // TODO: Will be handled in FMWK-486
    //@Test
    //void logicalOperators_functionCalls() {
    //    translateAndPrint("$.a.exists() and $.b.exists()");
    //}
}
