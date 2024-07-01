package com.aerospike;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.aerospike.TestUtils.translateAndPrint;
import static com.aerospike.TestUtils.translatePrintAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RecordAndBinExpressionsTests {

    @Test
    void binGT() {
        translatePrintAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        translatePrintAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        translatePrintAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

    }

    @Test
    void binEquals() {
        translatePrintAndCompare("$.intBin1 == 100", Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        translatePrintAndCompare("$.strBin == \"yes\"", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        translatePrintAndCompare("$.strBin == 'yes'", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void stringBinEqualsNegativeTest() {
        assertThatThrownBy(() -> translateAndPrint("$.strBin == yes"))
                .isInstanceOf(NumberFormatException.class)
                .hasMessage("For input string: \"yes\"");
    }

    @Test
    void logicalOperators_SimpleBinComparison() {
        Exp testExp1 = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        translatePrintAndCompare("$.intBin1 > 100 and $.intBin2 > 100", testExp1);

        Exp testExp2 = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
        );
        translatePrintAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100", testExp2); // TODO: what should be the default behaviour with no parentheses?
        translatePrintAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100", testExp2);

        Exp testExp3 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
                )
        );
        translatePrintAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", testExp3);

        // check that parentheses make difference
        assertThatThrownBy(() -> translatePrintAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", testExp2))
                .isInstanceOf(AssertionFailedError.class);
    }

    @Test
    void logicalOperators_functionCalls() {
        translateAndPrint("$.a.exists() and $.b.exists()");
    }
}