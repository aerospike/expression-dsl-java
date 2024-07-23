package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translate;
import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void floatToIntComparison() {
        translateAndCompare("$.intBin1.get(type: INT) > $.floatBin1.asInt()",
                Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1")));
    }

    @Test
    void intToFloatComparison() {
        translateAndCompare("$.intBin1.get(type: INT) > $.intBin2.asFloat()",
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    // TODO: Should fail
    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> translate("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }
}
