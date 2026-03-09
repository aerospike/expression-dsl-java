package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.PlaceholderValues;
import org.junit.jupiter.api.Test;

import java.util.List;
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
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToInt() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(42))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToFloat() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(1.5))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToBool() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(true))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToMap() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0",
                        PlaceholderValues.of(Map.of("a", 1)))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negLiteralInMixedTypeList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("\"x\" in [1, \"y\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negPlaceholderInMixedTypeList() {
        // Placeholder value is irrelevant: homogeneity validation fires at parse time before resolution
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("?0 in [1, \"y\"]",
                        PlaceholderValues.of(42))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negPathInMixedTypeList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.name in [1, \"y\"]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedTypeListViaPlaceholder() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?0",
                        PlaceholderValues.of(List.of(1, "hello")))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negVariableInMixedTypeList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("with(x = 1) do (${x} in [1, \"y\"])")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    // --- Ambiguous left operand ---

    @Test
    void negBinInBinAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.itemType in $.allowedItems")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInPathAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.rooms.room1.name")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInListDesignatorAmb() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.binName.[]")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInPlaceholderAmb() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInVariableAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("with(x = [\"a\"]) do ($.name in ${x})")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPathInBinAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.name in $.list")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPathInPathAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.a in $.rooms.room2.b")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPathInPlaceholderAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.name in ?0")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negExplicitBinInNotList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?0",
                        PlaceholderValues.of("Bob"))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("placeholder ?0");
    }

    @Test
    void negPlaceholderInNotList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("?0 in ?1",
                        PlaceholderValues.of("gold", "notAList"))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("placeholder ?1");
    }

    // --- Missing placeholder values ---

    @Test
    void negPlaceholderMissingValue() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?0", PlaceholderValues.of())))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Missing value for placeholder ?0");
    }

    @Test
    void negPlaceholderIndexOutOfBounds() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?1", PlaceholderValues.of(42))))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Missing value for placeholder ?1");
    }
}
