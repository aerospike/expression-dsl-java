package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.dsl.ConditionTranslator;
import lombok.experimental.UtilityClass;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    public static void translate(String input) {
        ConditionTranslator.translate(input);
    }

    public static void translateAndCompare(String input, Exp testExp) {
        Expression expression = ConditionTranslator.translate(input);
        Expression testExpr = Exp.build(testExp);
        assertEquals(expression, testExpr);
    }
}
