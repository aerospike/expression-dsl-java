package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseExpression;
import static com.aerospike.dsl.util.TestUtils.parseExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void floatToIntComparison() {
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1"));
        // Int is default
        parseExpressionAndCompare("$.intBin1 > $.floatBin1.asInt()", expectedExp);
        parseExpressionAndCompare("$.intBin1.get(type: INT) > $.floatBin1.asInt()", expectedExp);
    }

    @Test
    void intToFloatComparison() {
        parseExpressionAndCompare("$.intBin1.get(type: INT) > $.intBin2.asFloat()",
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> parseExpression("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }
}
