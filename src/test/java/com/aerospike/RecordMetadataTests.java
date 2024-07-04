package com.aerospike;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.TestUtils.translateAndCompare;

public class RecordMetadataTests {

    @Test
    void deviceSize() {
        // Expression to find records that occupy more than 1 MiB of storage space
        String input = "$.deviceSize() > 1048576";
        Exp testExp = Exp.gt(Exp.deviceSize(), Exp.val(1024 * 1024));
        translateAndCompare(input, testExp);
    }

    @Test
    void memorySize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.memorySize() > 1048576";
        Exp testExp = Exp.gt(Exp.memorySize(), Exp.val(1024 * 1024));
        translateAndCompare(input, testExp);
    }

    @Test
    void recordSize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.recordSize() > 1048576";
        Exp testExp = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        translateAndCompare(input, testExp);
    }

    @Test
    void digestModulo() {
        // Expression to find records where digest mod 3 equals 0
        String input = "$.digestModulo(3) == 0";
        Exp testExp = Exp.eq(Exp.digestModulo(3), Exp.val(0));
        translateAndCompare(input, testExp);
    }

    @Test
    void isTombstone() {
        // Expression to find records that are tombstones
        String input = "$.isTombstone()";
        Exp testExp = Exp.isTombstone();
        translateAndCompare(input, testExp);
    }

    @Test
    void keyExists() {
        // Expression to find records that has a stored key
        String input = "$.keyExists()";
        Exp testExp = Exp.keyExists();
        translateAndCompare(input, testExp);
    }

    // Comparing Metadata to a Bin
    @Test
    void lastUpdate() {
        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String inputMetadataLeft = "$.lastUpdate() < $.updateBy";
        Exp testExpLeft = Exp.lt(Exp.lastUpdate(), Exp.intBin("updateBy"));
        translateAndCompare(inputMetadataLeft, testExpLeft);

        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String inputMetadataRight = "$.updateBy > $.lastUpdate()";
        Exp testExpRight = Exp.gt(Exp.intBin("updateBy"), Exp.lastUpdate());
        translateAndCompare(inputMetadataRight, testExpRight);
    }

    @Test
    void sinceUpdate() {
        // Expression to find records that were updated within the last 2 hours
        String input = "$.sinceUpdate() < 7200000";
        Exp testExp = Exp.lt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000));
        translateAndCompare(input, testExp);
    }

    @Test
    void setName() {
        // Expression to find records where the set_name is either 'groupA' or 'groupB'
        String input = "$.setName() == \"groupA\" or $.setName() == \"groupB\"";
        Exp testExp = Exp.or(
                Exp.eq(Exp.setName(), Exp.val("groupA")),
                Exp.eq(Exp.setName(), Exp.val("groupB"))
        );
        translateAndCompare(input, testExp);
    }

    @Test
    void ttl() {
        // Expression to find records that will expire within 24 hours
        String input = "$.ttl() <= 86400";
        Exp testExp = Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60));
        translateAndCompare(input, testExp);
    }

    @Test
    void voidTime() {
        // Expression to find records where the void-time is set to 'never expire'
        String input = "$.voidTime() == -1";
        Exp testExp = Exp.eq(Exp.voidTime(), Exp.val(-1));
        translateAndCompare(input, testExp);
    }

    @Test
    void metadataWithLogicalOperatorsExpressions() {
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
