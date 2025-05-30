package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static com.aerospike.dsl.util.TestUtils.parseDslExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogicalExpressionsTests {

    @Test
    void binLogicalAndOrCombinations() {
        Exp expected1 = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", expected1);

        Exp expected2 = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
        );
        // TODO: what should be the default behaviour with no parentheses?
        TestUtils.parseFilterExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100", expected2);
        TestUtils.parseFilterExpressionAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100", expected2);
        TestUtils.parseFilterExpressionAndCompare("(($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100)", expected2);

        Exp expected3 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
                )
        );
        TestUtils.parseFilterExpressionAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", expected3);
        TestUtils.parseFilterExpressionAndCompare("$.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100)", expected3);
        // check that parentheses make difference
        assertThatThrownBy(
                () -> TestUtils.parseFilterExpressionAndCompare("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))", expected2)
        ).isInstanceOf(AssertionFailedError.class);
    }

    @Test
    void logicalNot() {
        TestUtils.parseFilterExpressionAndCompare("not($.keyExists())", Exp.not(Exp.keyExists()));
    }

    @Test
    void binLogicalExclusive() {
        TestUtils.parseFilterExpressionAndCompare("exclusive($.hand == \"hook\", $.leg == \"peg\")",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")),
                        Exp.eq(Exp.stringBin("leg"), Exp.val("peg"))));

        // More than 2 expressions exclusive
        TestUtils.parseFilterExpressionAndCompare("exclusive($.a == \"aVal\", $.b == \"bVal\", $.c == \"cVal\", $.d == 4)",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("a"), Exp.val("aVal")),
                        Exp.eq(Exp.stringBin("b"), Exp.val("bVal")),
                        Exp.eq(Exp.stringBin("c"), Exp.val("cVal")),
                        Exp.eq(Exp.intBin("d"), Exp.val(4))));
    }

    //TODO: FMWK-488
    //@Test
    void flatHierarchyAnd() {
        TestUtils.parseFilterExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 < 100",
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
        assertThatThrownBy(() -> parseFilterExp("($.intBin1 > 100 and ($.intBin2 > 100) or"))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input");

        assertThatThrownBy(() -> parseFilterExp("and ($.intBin1 > 100 and ($.intBin2 > 100)"))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input");

        assertThatThrownBy(() -> parseFilterExp("($.intBin1 > 100 and ($.intBin2 > 100) not"))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input");

        assertThatThrownBy(() -> parseFilterExp("($.intBin1 > 100 and ($.intBin2 > 100) exclusive"))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input");
    }

    @Test
    void negativeBinLogicalExclusiveWithOneParam() {
        assertThatThrownBy(() -> TestUtils.parseFilterExpressionAndCompare("exclusive($.hand == \"hook\")",
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")))))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Exclusive logical operator requires 2 or more expressions");
    }
}
