package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseExpression;
import static com.aerospike.dsl.util.TestUtils.parseExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinExpressionsTests {

    @Test
    void binGT() {
        parseExpressionAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpressionAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpressionAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

        parseExpressionAndCompare("100 < $.intBin1", Exp.lt(Exp.val(100), Exp.intBin("intBin1")));
        parseExpressionAndCompare("'text' < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
        parseExpressionAndCompare("\"text\" < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
    }

    @Test
    void binGE() {
        parseExpressionAndCompare("$.intBin1 >= 100", Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpressionAndCompare("$.stringBin1 >= 'text'", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpressionAndCompare("$.stringBin1 >= \"text\"", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLT() {
        parseExpressionAndCompare("$.intBin1 < 100", Exp.lt(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpressionAndCompare("$.stringBin1 < 'text'", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpressionAndCompare("$.stringBin1 < \"text\"", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLE() {
        parseExpressionAndCompare("$.intBin1 <= 100", Exp.le(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpressionAndCompare("$.stringBin1 <= 'text'", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpressionAndCompare("$.stringBin1 <= \"text\"", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binEquals() {
        parseExpressionAndCompare("$.intBin1 == 100", Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpressionAndCompare("$.strBin == \"yes\"", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        parseExpressionAndCompare("$.strBin == 'yes'", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void binNotEquals() {
        parseExpressionAndCompare("$.intBin1 != 100", Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpressionAndCompare("$.strBin != \"yes\"", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        parseExpressionAndCompare("$.strBin != 'yes'", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void negativeStringBinEquals() {
        assertThatThrownBy(() -> parseExpression("$.strBin == yes"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse right operand");
    }

    // TODO: Will be handled in FMWK-486
    //@Test
    //void logicalOperators_functionCalls() {
    //    translate("$.a.exists() and $.b.exists()");
    //}
}
