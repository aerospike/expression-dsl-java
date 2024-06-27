package com.aerospike;

import com.aerospike.client.exp.Expression;

public class ExpDslDemoApp {

    public static void main(String[] args) {
        testSimpleIntBinGT();
        testStringBinEquals();
        testAnd();
        testMetadata();
    }

    private static void testSimpleIntBinGT() {
        translateAndPrint("$.intBin1 > 10");
    }

    private static void testStringBinEquals() {
        translateAndPrint("$.strBin == \"yes\"");
    }

    private static void testAnd() {
        translateAndPrint("$.a.exists() and $.b.exists()");
    }

    private static void testMetadata() {
        translateAndPrint("$.deviceSize() > 1024 and $.ttl() < 300");
    }

    private static void translateAndPrint(String input) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
    }
}
