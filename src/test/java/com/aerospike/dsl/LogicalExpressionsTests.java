package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.aerospike.dsl.util.TestUtils.translate;
import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogicalExpressionsTests {

    @Test
    void binLogicalAndOrCombinations() {
        Exp expected1 = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100", expected1);

        Exp expected2 = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
        );
        // TODO: what should be the default behaviour with no parentheses?
        translateAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100", expected2);
        translateAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100", expected2);

        Exp expected3 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
                )
        );
        translateAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", expected3);

        // check that parentheses make difference
        assertThatThrownBy(() -> translateAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", expected2))
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

    @Test
    void negativeSyntaxLogicalOperators() {
        assertThatThrownBy(() -> translate("($.intBin1 > 100 and ($.intBin2 > 100) or"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Could not parse given input");

        assertThatThrownBy(() -> translate("and ($.intBin1 > 100 and ($.intBin2 > 100)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Could not parse given input");

        assertThatThrownBy(() -> translate("($.intBin1 > 100 and ($.intBin2 > 100) not"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Could not parse given input");

        assertThatThrownBy(() -> translate("($.intBin1 > 100 and ($.intBin2 > 100) exclusive"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Could not parse given input");
    }

    @Test
    void negativeBinLogicalExclusiveWithOneParam() {
        assertThatThrownBy(() -> translateAndCompare("exclusive($.hand == \"hook\")",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")))))
                .isInstanceOf(NullPointerException.class);
    }
}
