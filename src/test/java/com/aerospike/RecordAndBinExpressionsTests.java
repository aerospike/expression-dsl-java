package com.aerospike;

import com.aerospike.client.exp.Expression;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RecordAndBinExpressionsTests {

    @Test
    void intBinGT() {
        translateAndPrint("$.intBin1 > 10");
    }

    @Test
    void stringBinEquals() {
        translateAndPrint("$.strBin == \"yes\"");
        translateAndPrint("$.strBin == 'yes'");
    }

    @Test
    void stringBinEqualsNegativeTest() {
        assertThatThrownBy(() -> translateAndPrint("$.strBin == yes"))
                .isInstanceOf(NumberFormatException.class)
                .hasMessage("For input string: \"yes\"");
    }

    @Test
    void testAnd() {
        translateAndPrint("$.a.exists() and $.b.exists()");
    }

    private void translateAndPrint(String input) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
    }
}
