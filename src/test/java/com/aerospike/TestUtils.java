package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {

    static Expression translate(String input) {
        return ConditionTranslator.translate(input);
    }

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
