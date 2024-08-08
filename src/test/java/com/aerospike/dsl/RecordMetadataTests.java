package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translate;
import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordMetadataTests {

    @Test
    void deviceSize() {
        // Expression to find records that occupy more than 1 MiB of storage space
        String input = "$.deviceSize() > 1048576";
        Exp expected = Exp.gt(Exp.deviceSize(), Exp.val(1024 * 1024));
        translateAndCompare(input, expected);
    }

    @Test
    void memorySize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.memorySize() > 1048576";
        Exp expected = Exp.gt(Exp.memorySize(), Exp.val(1024 * 1024));
        translateAndCompare(input, expected);
    }

    @Test
    void recordSize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.recordSize() > 1048576";
        Exp expected = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        translateAndCompare(input, expected);
    }

    @Test
    void digestModulo() {
        // Expression to find records where digest mod 3 equals 0
        String input = "$.digestModulo(3) == 0";
        Exp expected = Exp.eq(Exp.digestModulo(3), Exp.val(0));
        translateAndCompare(input, expected);

        // Expression to find records where digest mod 3 equals the value stored in the bin called "digestModulo"
        String input2 = "$.digestModulo(3) == $.digestModulo";
        Exp expected2 = Exp.eq(Exp.digestModulo(3), Exp.intBin("digestModulo"));
        translateAndCompare(input2, expected2);
    }

    @Test
    void isTombstone() {
        // Expression to find records that are tombstones
        String input = "$.isTombstone()";
        Exp expected = Exp.isTombstone();
        translateAndCompare(input, expected);
    }

    @Test
    void keyExists() {
        // Expression to find records that has a stored key
        String input = "$.keyExists()";
        Exp expected = Exp.keyExists();
        translateAndCompare(input, expected);
    }

    // Comparing Metadata to a Bin
    @Test
    void lastUpdate() {
        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String inputMetadataLeft = "$.lastUpdate() < $.updateBy";
        Exp expectedLeft = Exp.lt(Exp.lastUpdate(), Exp.intBin("updateBy"));
        translateAndCompare(inputMetadataLeft, expectedLeft);

        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String inputMetadataRight = "$.updateBy > $.lastUpdate()";
        Exp expectedRight = Exp.gt(Exp.intBin("updateBy"), Exp.lastUpdate());
        translateAndCompare(inputMetadataRight, expectedRight);
    }

    @Test
    void sinceUpdate() {
        // Expression to find records that were updated within the last 2 hours
        String input = "$.sinceUpdate() < 7200000";
        Exp expected = Exp.lt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000));
        translateAndCompare(input, expected);

        // Expression to find records that were update within the value stored in the bin called "intBin"
        String input2 = "$.sinceUpdate() < $.intBin";
        Exp expected2 = Exp.lt(Exp.sinceUpdate(), Exp.intBin("intBin"));
        translateAndCompare(input2, expected2);

        // Expression to find records that were updated within the value stored in the bin called "sinceUpdate"
        String input3 = "$.sinceUpdate() < $.sinceUpdate";
        Exp expected3 = Exp.lt(Exp.sinceUpdate(), Exp.intBin("sinceUpdate"));
        translateAndCompare(input3, expected3);

        // Expression to find records that were updated within the value stored in the bin called "sinceUpdate"
        String input4 = "$.sinceUpdate > $.sinceUpdate()";
        Exp expected4 = Exp.gt(Exp.intBin("sinceUpdate"), Exp.sinceUpdate());
        translateAndCompare(input4, expected4);
    }

    @Test
    void setName() {
        // Expression to find records where the set_name is either 'groupA' or 'groupB'
        String input = "$.setName() == \"groupA\" or $.setName() == \"groupB\"";
        Exp expected = Exp.or(
                Exp.eq(Exp.setName(), Exp.val("groupA")),
                Exp.eq(Exp.setName(), Exp.val("groupB"))
        );
        translateAndCompare(input, expected);
    }

    @Test
    void ttl() {
        // Expression to find records that will expire within 24 hours
        String input = "$.ttl() <= 86400";
        Exp expected = Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60));
        translateAndCompare(input, expected);
    }

    //@Test
    void negativeTtlAsDifferentType() {
        // TODO: should be supported when adding operator + metadata validations (requires a refactor)
        assertThatThrownBy(() -> translate("$.ttl() == true"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Expecting non-bin operand, got BOOL_OPERAND");
    }

    @Test
    void voidTime() {
        // Expression to find records where the void-time is set to 'never expire'
        String input = "$.voidTime() == -1";
        Exp expected = Exp.eq(Exp.voidTime(), Exp.val(-1));
        translateAndCompare(input, expected);
    }

    @Test
    void metadataWithLogicalOperatorsExpressions() {
        // test AND
        String input = "$.deviceSize() > 1024 and $.ttl() < 300";
        Exp expected = Exp.and(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        translateAndCompare(input, expected);

        // test OR
        String input2 = "$.deviceSize() > 1024 or $.ttl() < 300";
        Exp expected2 = Exp.or(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        translateAndCompare(input2, expected2);
    }

    @Test
    void metadataAsExpressionWithLogicalOperator() {
        String input = "$.isTombstone() and $.ttl() < 300";
        Exp expected = Exp.and(
                Exp.isTombstone(),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        translateAndCompare(input, expected);

        input = "$.ttl() < 300 or $.keyExists()";
        expected = Exp.or(
                Exp.lt(Exp.ttl(), Exp.val(300)),
                Exp.keyExists()
        );
        translateAndCompare(input, expected);
    }
}
