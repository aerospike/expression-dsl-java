package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static com.aerospike.dsl.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InExplicitTypeTests {

    @Test
    void explicitListTypeOnRightBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.listBin("tags"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in $.tags.get(type: LIST)"), expected);
    }

    @Test
    void explicitIntInIntList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("age"), Exp.val(List.of(1, 2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.age.get(type: INT) in [1, 2]"), expected);
    }

    @Test
    void explicitStringInStringList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("a")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: STRING) in [\"a\"]"), expected);
    }

    @Test
    void explicitIntCompatibleWithFloatList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.val(List.of(1.5, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: INT) in [1.5, 2.5]"), expected);
    }

    @Test
    void explicitFloatCompatibleWithIntList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.val(List.of(1, 2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: FLOAT) in [1, 2]"), expected);
    }

    @Test
    void explicitFloatInFloatList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.val(List.of(1.5, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: FLOAT) in [1.5, 2.5]"), expected);
    }

    @Test
    void explicitBoolInBoolList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("flag"), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.flag.get(type: BOOL) in [true, false]"), expected);
    }

    @Test
    void negExplicitIntTypeOnRightBin() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.tags.get(type: INT)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negExplicitStringTypeOnRightBin() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.tags.get(type: STRING)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negNestedPathExplicitStringOnRight() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.tags.nested.get(type: STRING)")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negExplicitStringInIntList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in [1, 2]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitIntInStringList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.age.get(type: INT) in [\"a\", \"b\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitBoolInIntList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.flag.get(type: BOOL) in [1, 2]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitIntInBoolList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.val.get(type: INT) in [true, false]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitFloatInStringList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.val.get(type: FLOAT) in [\"a\", \"b\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitFloatInBoolList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.val.get(type: FLOAT) in [true, false]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitStringInFloatList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in [1.5, 2.5]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitStringInBoolList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in [true, false]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitBoolInStringList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.flag.get(type: BOOL) in [\"a\", \"b\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitBoolInFloatList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.flag.get(type: BOOL) in [1.5, 2.5]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare");
    }
}
