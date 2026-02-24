package com.aerospike.dsl.expression;

import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

public class ControlStructuresTests {

    @Test
    void whenWithASingleDeclaration() {
        Exp expected = Exp.cond(
                Exp.eq(
                        Exp.intBin("who"),
                        Exp.val(1)
                ), Exp.val("bob"),
                Exp.val("other")
        );

        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("when ($.who == 1 => \"bob\", default => \"other\")"),
                expected);
        // different spacing style
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("when($.who == 1 => \"bob\", default => \"other\")"),
                expected);
        // alternative quotation
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("when($.who == 1 => 'bob', default => 'other')"),
                expected);
    }

    @Test
    void whenUsingTheResult() {
        Exp expected = Exp.eq(
                Exp.stringBin("stringBin1"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("who"),
                                Exp.val(1)
                        ), Exp.val("bob"),
                        Exp.val("other")
                )
        );
        // TODO: FMWK-533 Implicit Type Detection for Control Structures
        // Implicit detect as String
        //translateAndCompare("$.stringBin1 == (when ($.who == 1 => \"bob\", default => \"other\"))",
        //        expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1.get(type: STRING) == " +
                        "(when ($.who == 1 => \"bob\", default => \"other\"))"),
                expected);
    }

    @Test
    void whenWithMultipleDeclarations() {
        Exp expected = Exp.cond(
                Exp.eq(
                        Exp.intBin("who"),
                        Exp.val(1)
                ), Exp.val("bob"),
                Exp.eq(
                        Exp.intBin("who"),
                        Exp.val(2)
                ), Exp.val("fred"),
                Exp.val("other")
        );

        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("when ($.who == 1 => \"bob\", " +
                        "$.who == 2 => \"fred\", default => \"other\")"),
                expected);
    }

    @Test
    void withMultipleVariablesDefinitionAndUsage() {
        Exp expected = Exp.let(
                Exp.def("x",
                        Exp.val(1)
                ),
                Exp.def("y",
                        Exp.add(Exp.var("x"), Exp.val(1))
                ),
                Exp.add(Exp.var("x"), Exp.var("y"))
        );

        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("with (x = 1, y = ${x} + 1) do (${x} + ${y})"),
                expected);
        // different spacing style
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("with(x = 1, y = ${x} + 1) do(${x} + ${y})"),
                expected);
    }
}
