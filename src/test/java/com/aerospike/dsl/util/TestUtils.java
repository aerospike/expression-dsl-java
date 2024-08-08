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

    public static void translateAndCompare(String input, Exp expected) {
        Expression actualExpression = ConditionTranslator.translate(input);
        Expression expectedExpression = Exp.build(expected);
        assertEquals(actualExpression, expectedExpression);
    }
}
