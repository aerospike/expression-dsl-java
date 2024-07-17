package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.TestUtils.translateAndCompare;

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
}
