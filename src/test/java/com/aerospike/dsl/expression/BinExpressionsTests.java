package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static com.aerospike.dsl.util.TestUtils.parseFilterExpAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinExpressionsTests {

    @Test
    void binGT() {
        parseFilterExpAndCompare("$.intBin1 > 100", Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpAndCompare("$.stringBin1 > 'text'", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpAndCompare("$.stringBin1 > \"text\"", Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));

        parseFilterExpAndCompare("100 < $.intBin1", Exp.lt(Exp.val(100), Exp.intBin("intBin1")));
        parseFilterExpAndCompare("'text' < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
        parseFilterExpAndCompare("\"text\" < $.stringBin1", Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
    }

    @Test
    void binGE() {
        parseFilterExpAndCompare("$.intBin1 >= 100", Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpAndCompare("$.stringBin1 >= 'text'", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpAndCompare("$.stringBin1 >= \"text\"", Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLT() {
        parseFilterExpAndCompare("$.intBin1 < 100", Exp.lt(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpAndCompare("$.stringBin1 < 'text'", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpAndCompare("$.stringBin1 < \"text\"", Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLE() {
        parseFilterExpAndCompare("$.intBin1 <= 100", Exp.le(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpAndCompare("$.stringBin1 <= 'text'", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpAndCompare("$.stringBin1 <= \"text\"", Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binEquals() {
        parseFilterExpAndCompare("$.intBin1 == 100", Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpAndCompare("$.strBin == \"yes\"", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        parseFilterExpAndCompare("$.strBin == 'yes'", Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void binNotEquals() {
        parseFilterExpAndCompare("$.intBin1 != 100", Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpAndCompare("$.strBin != \"yes\"", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        parseFilterExpAndCompare("$.strBin != 'yes'", Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void negativeStringBinEquals() {
        assertThatThrownBy(() -> parseFilterExp("$.strBin == yes"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse right operand");
    }

    // TODO: Will be handled in FMWK-486
    //@Test
    //void logicalOperators_functionCalls() {
    //    translate("$.a.exists() and $.b.exists()");
    //}
}
