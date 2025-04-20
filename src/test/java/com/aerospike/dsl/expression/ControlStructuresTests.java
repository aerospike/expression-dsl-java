package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExpAndCompare;

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

        parseFilterExpAndCompare("when ($.who == 1 => \"bob\", default => \"other\")",
                expected);
        // different spacing style
        parseFilterExpAndCompare("when($.who == 1 => \"bob\", default => \"other\")",
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
        parseFilterExpAndCompare("$.stringBin1.get(type: STRING) == (when ($.who == 1 => \"bob\", default => \"other\"))",
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

        parseFilterExpAndCompare("when ($.who == 1 => \"bob\", $.who == 2 => \"fred\", default => \"other\")",
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

        parseFilterExpAndCompare("with (x = 1, y = ${x} + 1) do (${x} + ${y})",
                expected);
        // different spacing style
        parseFilterExpAndCompare("with(x = 1, y = ${x}+1) do(${x}+${y})",
                expected);
    }
}
