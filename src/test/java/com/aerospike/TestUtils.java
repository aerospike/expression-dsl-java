package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {

    static void translateAndPrint(String input) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
    }

    static void translatePrintAndCompare(String input, Exp testExp) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
        Expression testExpr = Exp.build(testExp);
        int diff = Arrays.compare(expression.getBytes(), testExpr.getBytes());
        if (diff != 0) System.out.println("Difference in expressions' bytes: " + diff);
        assertEquals(expression, testExpr);
    }
}
