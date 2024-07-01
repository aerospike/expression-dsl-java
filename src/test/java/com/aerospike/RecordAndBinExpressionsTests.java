package com.aerospike;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.TestUtils.translateAndPrint;
import static com.aerospike.TestUtils.translatePrintAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RecordAndBinExpressionsTests {

    @Test
    void intBinGT() {
        translatePrintAndCompare("$.intBin1 > 10", Exp.gt(Exp.intBin("intBin1"), Exp.val(10)));
        translatePrintAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        translatePrintAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

    }

    @Test
    void stringBinEquals() {
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
    void testAnd_SimpleBinComparison() {
        Exp testExp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        translatePrintAndCompare("$.intBin1 > 100 and $.intBin2 > 100", testExp);
    }

    @Test
    void testAnd_functionCalls() {
        translateAndPrint("$.a.exists() and $.b.exists()");
    }
}
