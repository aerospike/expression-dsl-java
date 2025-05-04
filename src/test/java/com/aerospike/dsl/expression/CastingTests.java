package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exceptions.ParseException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseExp;
import static com.aerospike.dsl.util.TestUtils.parseExpAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void floatToIntComparison() {
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1"));
        // Int is default
        parseExpAndCompare("$.intBin1 > $.floatBin1.asInt()", expectedExp);
        parseExpAndCompare("$.intBin1.get(type: INT) > $.floatBin1.asInt()", expectedExp);
    }

    @Test
    void intToFloatComparison() {
        parseExpAndCompare("$.intBin1.get(type: INT) > $.intBin2.asFloat()",
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> parseExp("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()"))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }
}
