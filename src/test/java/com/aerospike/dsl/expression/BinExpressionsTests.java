package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.DslParseException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinExpressionsTests {

    @Test
    void binGT() {
        parseFilterExpressionAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

        parseFilterExpressionAndCompare("100 < $.intBin1", Exp.lt(Exp.val(100), Exp.intBin("intBin1")));
        parseFilterExpressionAndCompare("'text' < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
        parseFilterExpressionAndCompare("\"text\" < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
    }

    @Test
    void binGE() {
        parseFilterExpressionAndCompare("$.intBin1 >= 100", Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare("$.stringBin1 >= 'text'", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare("$.stringBin1 >= \"text\"", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLT() {
        parseFilterExpressionAndCompare("$.intBin1 < 100", Exp.lt(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare("$.stringBin1 < 'text'", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare("$.stringBin1 < \"text\"", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLE() {
        parseFilterExpressionAndCompare("$.intBin1 <= 100", Exp.le(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare("$.stringBin1 <= 'text'", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare("$.stringBin1 <= \"text\"", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binEquals() {
        parseFilterExpressionAndCompare("$.intBin1 == 100", Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare("$.strBin == \"yes\"", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        parseFilterExpressionAndCompare("$.strBin == 'yes'", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));

        parseFilterExpressionAndCompare("100 == $.intBin1", Exp.eq(Exp.val(100), Exp.intBin("intBin1")));
        parseFilterExpressionAndCompare("\"yes\" == $.strBin", Exp.eq(Exp.val("yes"), Exp.stringBin("strBin")));
        parseFilterExpressionAndCompare("'yes' == $.strBin", Exp.eq(Exp.val("yes"), Exp.stringBin("strBin")));
    }

    @Test
    void binNotEquals() {
        parseFilterExpressionAndCompare("$.intBin1 != 100", Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare("$.strBin != \"yes\"", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        parseFilterExpressionAndCompare("$.strBin != 'yes'", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void negativeStringBinEquals() {
        assertThatThrownBy(() -> parseFilterExp("$.strBin == yes"))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Unable to parse right operand");
    }

    // TODO: Will be handled in FMWK-486
    //@Test
    //void logicalOperators_functionCalls() {
    //    translate("$.a.exists() and $.b.exists()");
    //}
}
