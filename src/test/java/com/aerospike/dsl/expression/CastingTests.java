package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.InputContext;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void floatToIntComparison() {
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1"));
        // Int is default
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.intBin1 > $.floatBin1.asInt()"), expectedExp);
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.intBin1.get(type: INT) > $.floatBin1.asInt()"), expectedExp);
    }

    @Test
    void intToFloatComparison() {
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.intBin1.get(type: INT) > $.intBin2.asFloat()"),
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> parseFilterExp(InputContext.of("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }
}
