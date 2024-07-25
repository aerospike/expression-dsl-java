package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;

public class ControlStructuresTests {

    @Test
    void whenWithASingleDeclaration() {
        Exp expectedExp = Exp.cond(
                Exp.eq(
                        Exp.intBin("who"),
                        Exp.val(1)
                ), Exp.val("bob"),
                Exp.val("other")
        );

        translateAndCompare("when ($.who == 1 => \"bob\", default => \"other\")",
                expectedExp);
    }

    @Test
    void whenUsingTheResult() {
        Exp expectedExp = Exp.eq(
                Exp.stringBin("stringBin1"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("who"),
                                Exp.val(1)
                        ), Exp.val("bob"),
                        Exp.val("other")
                )
        );

        translateAndCompare("$.stringBin1.get(type: STRING) == (when ($.who == 1 => \"bob\", default => \"other\"))",
                expectedExp);
    }

    @Test
    void whenWithMultipleDeclarations() {
        Exp expectedExp = Exp.cond(
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

        translateAndCompare("when ($.who == 1 => \"bob\", $.who == 2 => \"fred\", default => \"other\")",
                expectedExp);
    }

    @Test
    void withMultipleVariablesDefinitionAndUsage() {
        Exp expectedExp = Exp.let(
                Exp.def("x",
                        Exp.val(1)
                ),
                Exp.def("y",
                        Exp.add(Exp.var("x"), Exp.val(1))
                ),
                Exp.add(Exp.var("x"), Exp.var("y"))
        );

        translateAndCompare("with (x = 1, y = ${x} + 1) do (${x} + ${y})",
                expectedExp);
    }
}
