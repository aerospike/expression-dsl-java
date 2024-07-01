package com.aerospike;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataTests {

    @Test
    void testDeviceSize() {
        // Expression to find records that occupy more than 1 MiB of storage space
        Expression exp = Exp.build(
                Exp.gt(Exp.deviceSize(), Exp.val(1024 * 1024)));

        String expressionDSL = "$.deviceSize() > 1048576";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testMemorySize() {
        // Expression to find records that occupy more than 1 MiB of memory
        Expression exp = Exp.build(
                Exp.gt(Exp.memorySize(), Exp.val(1024 * 1024)));

        String expressionDSL = "$.memorySize() > 1048576";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testRecordSize() {
        // Expression to find records that occupy more than 1 MiB of memory
        Expression exp = Exp.build(
                Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024)));

        String expressionDSL = "$.recordSize() > 1048576";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    // TODO: Support optional param in functionCall(<param>)
    //@Test
    void testDigestModulo() {
        // Expression to find records where digest mod 3 equals 0
        Expression exp = Exp.build(
                Exp.eq(Exp.digestModulo(3), Exp.val(0)));

        String expressionDSL = "$.digestModulo(3) == 0";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testIsTombstone() {
        // Expression to find records that are tombstones
        Expression exp = Exp.build(Exp.isTombstone());

        String expressionDSL = "$.isTombstone()";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testKeyExists() {
        // Expression to find records that has a stored key
        Expression exp = Exp.build(Exp.keyExists());

        String expressionDSL = "$.keyExists()";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testLastUpdate() {
        // Expression to find records where the last-update-time is less than bin 'updateBy'
        Expression exp = Exp.build(
                Exp.lt(Exp.lastUpdate(), Exp.intBin("updateBy")));

        String expressionDSL = "$.lastUpdate() < $.updateBy";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testSinceUpdate() {
        // Expression to find records that were updated within the last 2 hours
        Expression exp = Exp.build(
                Exp.lt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000)));

        String expressionDSL = "$.sinceUpdate() < 7200000";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testSetName() {
        // Expression to find records where the set_name is either 'groupA' or 'groupB'
        Expression exp = Exp.build(Exp.or(
                Exp.eq(Exp.setName(), Exp.val("groupA")),
                Exp.eq(Exp.setName(), Exp.val("groupB"))));

        String expressionDSL = "$.setName() == \"groupA\" or $.setName() == \"groupB\"";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testTtl() {
        // Expression to find records that will expire within 24 hours
        Expression exp = Exp.build(
                Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60)));

        String expressionDSL = "$.ttl() <= 86400";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    // TODO: Is -1 (negative integers) supported out of the box?
    @Test
    void testVoidTime() {
        // Expression to find records where the void-time is set to 'never expire'
        Expression exp = Exp.build(Exp.eq(Exp.voidTime(), Exp.val(-1)));

        String expressionDSL = "$.voidTime() == -1";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    @Test
    void testTwoMetadataExpressions() {
        Expression exp = Exp.build(Exp.and(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))));

        String expressionDSL = "$.deviceSize() > 1024 and $.ttl() < 300";
        Expression generatedExp = translate(expressionDSL);
        assertEquals(exp, generatedExp);
    }

    private Expression translate(String input) {
        return ConditionTranslator.translate(input);
    }
}
