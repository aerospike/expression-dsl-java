package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static com.aerospike.dsl.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SyntaxErrorTests {

    // --- General syntax errors ---

    @Test
    void negNonsensePathFunction() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("1.0 == $.f1.nonsense()")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("extraneous input '()'");
    }

    @Test
    void negNonsensePathFunctionOnLeft() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.f1.nonsense() == 1.0")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("mismatched input '()'");
    }

    @Test
    void negTrailingGarbageTokens() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.intBin1 > 100 garbage")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("extraneous input 'garbage'");
    }

    // --- Positive syntax boundary ---

    @Test
    void letWithAlphanumericVarName() {
        Exp expected = Exp.let(
                Exp.def("var_1", Exp.val(5)),
                Exp.add(Exp.var("var_1"), Exp.val(1))
        );
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(var_1 = 5) then (${var_1} + 1)"), expected);
    }

    // --- Malformed variable references ---

    @Test
    void negVarBareNameInThenBody() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x})) then (y == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarDollarNoInThenBody() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x})) then ($y == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarMissingCloseBrace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x)) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarMissingDollarAndOpen() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in x})) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarBareNameInLetExpr() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in x)) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarDoubleOpenBrace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${{x})) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarDoubleCloseBrace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x}})) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negVarDoubleDollarSign() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in $${x})) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    // --- Mismatched delimiters ---

    @Test
    void negOrphanedParentheses() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.intBin1 > ()")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("mismatched input '()'");
    }

    @Test
    void negGetFuncMissingCloseParen() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT in ${x})) then (${y} == true)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    // --- Malformed let structure ---

    @Test
    void negLetMissingThen() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = 5) (${x} + 1)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negLetEmptyDefs() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let() then (1 + 2)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negLetMissingEquals() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x 5) then (${x} + 1)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    // --- Malformed when structure ---

    @Test
    void negWhenMissingDefault() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.x == 1 => \"a\")")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }

    @Test
    void negWhenMissingArrow() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.x == 1 \"a\", default => \"b\")")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse given DSL expression input")
                .hasMessageContaining("no viable alternative at input");
    }
}
