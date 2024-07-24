package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogicalExpressionsTests {

    @Test
    void binLogicalAndOrCombinations() {
        Exp testExp1 = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100", testExp1);

        Exp testExp2 = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
        );
        // TODO: what should be the default behaviour with no parentheses?
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100", testExp2);
        translateAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100", testExp2);

        Exp testExp3 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
                )
        );
        translateAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", testExp3);

        // check that parentheses make difference
        assertThatThrownBy(() -> translateAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", testExp2))
                .isInstanceOf(AssertionFailedError.class);
    }

    @Test
    void logicalNot() {
        translateAndCompare("not($.keyExists())", Exp.not(Exp.keyExists()));
    }

    @Test
    void binLogicalExclusive() {
        translateAndCompare("exclusive($.hand == \"hook\", $.leg == \"peg\")",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")),
                        Exp.eq(Exp.stringBin("leg"), Exp.val("peg"))));
    }

    //TODO: FMWK-488
    //@Test
    void flatHierarchyAnd() {
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 < 100",
                Exp.and(
                        Exp.gt(
                                Exp.intBin("intBin1"),
                                Exp.val(100)),
                        Exp.gt(
                                Exp.intBin("intBin2"),
                                Exp.val(100)
                        ),
                        Exp.lt(Exp.intBin("intBin3"),
                                Exp.val(100))));
    }
}
