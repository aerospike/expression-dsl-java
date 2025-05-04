package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exceptions.ParseException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseExp;
import static com.aerospike.dsl.util.TestUtils.parseExpAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinExpressionsTests {

    @Test
    void binGT() {
        parseExpAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

        parseExpAndCompare("100 < $.intBin1", Exp.lt(Exp.val(100), Exp.intBin("intBin1")));
        parseExpAndCompare("'text' < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
        parseExpAndCompare("\"text\" < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
    }

    @Test
    void binGE() {
        parseExpAndCompare("$.intBin1 >= 100", Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpAndCompare("$.stringBin1 >= 'text'", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpAndCompare("$.stringBin1 >= \"text\"", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLT() {
        parseExpAndCompare("$.intBin1 < 100", Exp.lt(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpAndCompare("$.stringBin1 < 'text'", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpAndCompare("$.stringBin1 < \"text\"", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLE() {
        parseExpAndCompare("$.intBin1 <= 100", Exp.le(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpAndCompare("$.stringBin1 <= 'text'", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseExpAndCompare("$.stringBin1 <= \"text\"", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binEquals() {
        parseExpAndCompare("$.intBin1 == 100", Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpAndCompare("$.strBin == \"yes\"", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        parseExpAndCompare("$.strBin == 'yes'", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void binNotEquals() {
        parseExpAndCompare("$.intBin1 != 100", Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        parseExpAndCompare("$.strBin != \"yes\"", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        parseExpAndCompare("$.strBin != 'yes'", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void negativeStringBinEquals() {
        assertThatThrownBy(() -> parseExp("$.strBin == yes"))
                .isInstanceOf(ParseException.class)
                .hasMessage("Unable to parse right operand");
    }

    // TODO: Will be handled in FMWK-486
    //@Test
    //void logicalOperators_functionCalls() {
    //    translate("$.a.exists() and $.b.exists()");
    //}
}
