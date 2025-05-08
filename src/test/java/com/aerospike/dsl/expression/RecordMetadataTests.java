package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static com.aerospike.dsl.util.TestUtils.parseDslExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordMetadataTests {

    @Test
    void deviceSize() {
        // Expression to find records that occupy more than 1 MiB of storage space
        String input = "$.deviceSize() > 1048576";
        Exp expected = Exp.gt(Exp.deviceSize(), Exp.val(1024 * 1024));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void memorySize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.memorySize() > 1048576";
        Exp expected = Exp.gt(Exp.memorySize(), Exp.val(1024 * 1024));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void recordSize() {
        // Expression to find records that occupy more than 1 MiB of memory
        String input = "$.recordSize() > 1048576";
        Exp expected = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void digestModulo() {
        // Expression to find records where digest mod 3 equals 0
        String input = "$.digestModulo(3) == 0";
        Exp expected = Exp.eq(Exp.digestModulo(3), Exp.val(0));
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // Expression to find records where digest mod 3 equals the value stored in the bin called "digestModulo"
        String input2 = "$.digestModulo(3) == $.digestModulo";
        Exp expected2 = Exp.eq(Exp.digestModulo(3), Exp.intBin("digestModulo"));
        TestUtils.parseFilterExpressionAndCompare(input2, expected2);
    }

    @Test
    void isTombstone() {
        // Expression to find records that are tombstones
        String input = "$.isTombstone()";
        Exp expected = Exp.isTombstone();
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void keyExists() {
        // Expression to find records that has a stored key
        String input = "$.keyExists()";
        Exp expected = Exp.keyExists();
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    // Comparing Metadata to a Bin
    @Test
    void lastUpdate() {
        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String inputMetadataLeft = "$.lastUpdate() < $.updateBy";
        Exp expectedLeft = Exp.lt(Exp.lastUpdate(), Exp.intBin("updateBy"));
        TestUtils.parseFilterExpressionAndCompare(inputMetadataLeft, expectedLeft);

        // Expression to find records where the last-update-time is less than bin 'updateBy'
        String inputMetadataRight = "$.updateBy > $.lastUpdate()";
        Exp expectedRight = Exp.gt(Exp.intBin("updateBy"), Exp.lastUpdate());
        TestUtils.parseFilterExpressionAndCompare(inputMetadataRight, expectedRight);
    }

    @Test
    void sinceUpdate() {
        // Expression to find records that were updated within the last 2 hours
        String input = "$.sinceUpdate() < 7200000";
        Exp expected = Exp.lt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000));
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // Expression to find records that were update within the value stored in the bin called "intBin"
        String input2 = "$.sinceUpdate() < $.intBin";
        Exp expected2 = Exp.lt(Exp.sinceUpdate(), Exp.intBin("intBin"));
        TestUtils.parseFilterExpressionAndCompare(input2, expected2);

        // Expression to find records that were updated within the value stored in the bin called "sinceUpdate"
        String input3 = "$.sinceUpdate() < $.sinceUpdate";
        Exp expected3 = Exp.lt(Exp.sinceUpdate(), Exp.intBin("sinceUpdate"));
        TestUtils.parseFilterExpressionAndCompare(input3, expected3);

        // Expression to find records that were updated within the value stored in the bin called "sinceUpdate"
        String input4 = "$.sinceUpdate > $.sinceUpdate()";
        Exp expected4 = Exp.gt(Exp.intBin("sinceUpdate"), Exp.sinceUpdate());
        TestUtils.parseFilterExpressionAndCompare(input4, expected4);
    }

    @Test
    void setName() {
        // Expression to find records where the set_name is either 'groupA' or 'groupB'
        String input = "$.setName() == \"groupA\" or $.setName() == \"groupB\"";
        Exp expected = Exp.or(
                Exp.eq(Exp.setName(), Exp.val("groupA")),
                Exp.eq(Exp.setName(), Exp.val("groupB"))
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // set name compared with String Bin
        input = "$.mySetBin == $.setName()";
        expected = Exp.eq(Exp.stringBin("mySetBin"), Exp.setName());
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void ttl() {
        // Expression to find records that will expire within 24 hours
        String input = "$.ttl() <= 86400";
        Exp expected = Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    //@Test
    void negativeTtlAsDifferentType() {
        // TODO: should be supported when adding operator + metadata validations (requires a refactor)
        assertThatThrownBy(() -> parseFilterExp("$.ttl() == true"))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Expecting non-bin operand, got BOOL_OPERAND");
    }

    @Test
    void voidTime() {
        // Expression to find records where the void-time is set to 'never expire'
        String input = "$.voidTime() == -1";
        Exp expected = Exp.eq(Exp.voidTime(), Exp.val(-1));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void metadataWithLogicalOperatorsExpressions() {
        // test AND
        String input = "$.deviceSize() > 1024 and $.ttl() < 300";
        Exp expected = Exp.and(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // test OR
        String input2 = "$.deviceSize() > 1024 or $.ttl() < 300";
        Exp expected2 = Exp.or(
                Exp.gt(Exp.deviceSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        TestUtils.parseFilterExpressionAndCompare(input2, expected2);
    }

    @Test
    void metadataAsExpressionWithLogicalOperator() {
        String input = "$.isTombstone() and $.ttl() < 300";
        Exp expected = Exp.and(
                Exp.isTombstone(),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        input = "$.ttl() < 300 or $.keyExists()";
        expected = Exp.or(
                Exp.lt(Exp.ttl(), Exp.val(300)),
                Exp.keyExists()
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }
}
