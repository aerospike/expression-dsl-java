package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import lombok.experimental.UtilityClass;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    static void translateAndPrint(String input) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
    }

    static void translateAndCompare(String input, Exp testExp) {
        Expression expression = ConditionTranslator.translate(input);
        Expression testExpr = Exp.build(testExp);
        assertEquals(expression, testExpr);
    }
}
