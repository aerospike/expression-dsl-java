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
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1"));
        // Int is default
        translateAndCompare("$.intBin1 > $.floatBin1.asInt()", expectedExp);
        translateAndCompare("$.intBin1.get(type: INT) > $.floatBin1.asInt()", expectedExp);
    }

    @Test
    void intToFloatComparison() {
        translateAndCompare("$.intBin1.get(type: INT) > $.intBin2.asFloat()",
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> translate("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }
}
