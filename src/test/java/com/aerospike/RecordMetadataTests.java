package com.aerospike;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.TestUtils.translateAndCompare;

public class RecordMetadataTests {

    @Test
    void testDeviceSize() {
        // Expression to find records that occupy more than 1 MiB of storage space
        String input = "$.deviceSize() > 1048576";
        Exp testExp = Exp.gt(Exp.deviceSize(), Exp.val(1024 * 1024));
        translateAndCompare(input, testExp);
    }

    @Test
    void testMemorySize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.memorySize() > 1048576";
        Exp testExp = Exp.gt(Exp.memorySize(), Exp.val(1024 * 1024));
        translateAndCompare(input, testExp);
    }

    @Test
    void testRecordSize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.recordSize() > 1048576";
        Exp testExp = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        translateAndCompare(input, testExp);

    }

    @Test
    void testDigestModulo() {
        // Expression to find records where digest mod 3 equals 0
        String input = "$.digestModulo(3) == 0";
        Exp testExp = Exp.eq(Exp.digestModulo(3), Exp.val(0));
        translateAndCompare(input, testExp);

    }

    @Test
    void testIsTombstone() {
        // Expression to find records that are tombstones
        String input = "$.isTombstone()";
        Exp testExp = Exp.isTombstone();
        translateAndCompare(input, testExp);

    }

    @Test
    void testKeyExists() {
        // Expression to find records that has a stored key
        String input = "$.keyExists()";
        Exp testExp = Exp.keyExists();
        translateAndCompare(input, testExp);
    }

    @Test
    void testLastUpdate() {
        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String input = "$.lastUpdate() < $.updateBy";
        Exp testExp = Exp.lt(Exp.lastUpdate(), Exp.intBin("updateBy"));
        translateAndCompare(input, testExp);
    }

    @Test
    void testSinceUpdate() {
        // Expression to find records that were updated within the last 2 hours
        String input = "$.sinceUpdate() < 7200000";
        Exp testExp = Exp.lt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000));
        translateAndCompare(input, testExp);
    }

    @Test
    void testSetName() {
        // Expression to find records where the set_name is either 'groupA' or 'groupB'
        String input = "$.setName() == \"groupA\" or $.setName() == \"groupB\"";
        Exp testExp = Exp.or(
                Exp.eq(Exp.setName(), Exp.val("groupA")),
                Exp.eq(Exp.setName(), Exp.val("groupB"))
        );
        translateAndCompare(input, testExp);
    }

    @Test
    void testTtl() {
        // Expression to find records that will expire within 24 hours
        String input = "$.ttl() <= 86400";
        Exp testExp = Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60));
        translateAndCompare(input, testExp);
    }

    @Test
    void testVoidTime() {
        // Expression to find records where the void-time is set to 'never expire'
        String input = "$.voidTime() == -1";
        Exp testExp = Exp.eq(Exp.voidTime(), Exp.val(-1));
        translateAndCompare(input, testExp);
    }

    @Test
    void testMetadataWithLogicalOperatorsExpressions() {
        // test AND
        String input = "$.deviceSize() > 1024 and $.ttl() < 300";
        Exp testExp = Exp.and(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        translateAndCompare(input, testExp);

        // test OR
        String input2 = "$.deviceSize() > 1024 or $.ttl() < 300";
        Exp testExp2 = Exp.or(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        translateAndCompare(input2, testExp2);
    }
}
