package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.PlaceholderValues;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InNegativeTests {

    @Test
    void negativeRightOperandString() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in \"Bob\"")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandInt() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in 42")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandFloat() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in 1.5")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandBool() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in true")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandMap() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in {\"a\": 1}")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandMetadata() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in $.ttl()")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negMixedIntAndStringInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [1, \"hello\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedBoolAndIntInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [true, 42]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedFloatAndStringInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [1.5, \"hello\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedIntAndFloatInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [1, 1.5]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negPlaceholderResolvesToStr() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of("Bob"))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negPlaceholderResolvesToInt() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(42))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negPlaceholderResolvesToFloat() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(1.5))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negPlaceholderResolvesToBool() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(true))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negPlaceholderResolvesToMap() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0",
                        PlaceholderValues.of(Map.of("a", 1)))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }
}
