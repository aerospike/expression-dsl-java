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
        translateAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        translateAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binGE() {
        translateAndCompare("$.intBin1 >= 100", Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.stringBin1 >= 'text'", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        translateAndCompare("$.stringBin1 >= \"text\"", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
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
    }

    @Test
    void binNotEquals() {
        translateAndCompare("$.intBin1 != 100", Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        translateAndCompare("$.strBin != \"yes\"", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        translateAndCompare("$.strBin != 'yes'", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void stringBinEqualsNegativeTest() {
        assertThatThrownBy(() -> translateAndPrint("$.strBin == yes"))
                .isInstanceOf(NumberFormatException.class)
                .hasMessage("For input string: \"yes\"");
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
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100", testExp2); // TODO: what should be the default behaviour with no parentheses?
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
    void binLogicalNot() {
        translateAndCompare("not($.keyExists())", Exp.not(Exp.keyExists()));
    }

    @Test
    void logicalOperators_functionCalls() {
        translateAndPrint("$.a.exists() and $.b.exists()");
    }
}
