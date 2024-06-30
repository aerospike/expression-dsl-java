package com.aerospike;

import com.aerospike.client.exp.Expression;
import org.junit.jupiter.api.Test;

public class MetadataTests {

    @Test
    void testDeviceSizeAndTtl() {
        translateAndPrint("$.deviceSize() > 1024 and $.ttl() < 300");
    }

    private void translateAndPrint(String input) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
    }
}
