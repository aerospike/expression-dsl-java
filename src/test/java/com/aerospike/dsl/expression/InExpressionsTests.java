package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static com.aerospike.dsl.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InExpressionsTests {

    @Test
    void stringLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.val(List.of("gold", "silver")));
        parseFilterExpressionAndCompare(ExpressionContext.of("\"gold\" in [\"gold\", \"silver\"]"), expected);
    }

    @Test
    void intLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(100), Exp.val(List.of(100, 200, 300)));
        parseFilterExpressionAndCompare(ExpressionContext.of("100 in [100, 200, 300]"), expected);
    }

    @Test
    void floatLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(1.5), Exp.val(List.of(1.0, 2.0, 3.0)));
        parseFilterExpressionAndCompare(ExpressionContext.of("1.5 in [1.0, 2.0, 3.0]"), expected);
    }

    @Test
    void boolLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(true), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(ExpressionContext.of("true in [true, false]"), expected);
    }

    @Test
    void listLiteralInListOfLists() {
        List<List<Integer>> outerList = List.of(
                List.of(2, 3, 4), List.of(3, 4, 5), List.of(1, 2, 3), List.of(1, 2));
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(List.of(1, 2, 3)), Exp.val(outerList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("[1,2,3] in [[2,3,4], [3,4,5], [1,2,3], [1,2]]"), expected);
    }

    @Test
    void listBinInListOfLists() {
        List<List<Integer>> outerList = List.of(
                List.of(2, 3, 4), List.of(3, 4, 5), List.of(1, 2, 3), List.of(1, 2));
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.listBin("listBin"), Exp.val(outerList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin in [[2,3,4], [3,4,5], [1,2,3], [1,2]]"), expected);
    }

    @Test
    void mapLiteralInListOfMaps() {
        TreeMap<Integer, String> map = new TreeMap<>();
        map.put(1, "a");
        TreeMap<Integer, String> map2 = new TreeMap<>();
        map2.put(2, "b");
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(map), Exp.val(List.of(map, map2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("{1: \"a\"} in [{1: \"a\"}, {2: \"b\"}]"), expected);
    }

    @Test
    void mapBinInListOfMaps() {
        TreeMap<Integer, String> map1 = new TreeMap<>();
        map1.put(1, "a");
        TreeMap<Integer, String> map2 = new TreeMap<>();
        map2.put(2, "b");
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.mapBin("mapBin"), Exp.val(List.of(map1, map2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin in [{1: \"a\"}, {2: \"b\"}]"), expected);
    }

    @Test
    void binInStringListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name in [\"Bob\", \"Mary\"]"), expected);
    }

    @Test
    void binInIntListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("age"), Exp.val(List.of(18, 21, 65)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.age in [18, 21, 65]"), expected);
    }

    @Test
    void binInFloatListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("score"), Exp.val(List.of(1.0, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.score in [1.0, 2.5]"), expected);
    }

    @Test
    void binInBoolListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("isActive"), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.isActive in [true, false]"), expected);
    }

    @Test
    void stringLiteralInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.listBin("allowedStatuses"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("\"gold\" in $.allowedStatuses"), expected);
    }

    @Test
    void intLiteralInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(100), Exp.listBin("allowedValues"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("100 in $.allowedValues"), expected);
    }

    @Test
    void floatLiteralInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(1.5), Exp.listBin("scores"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("1.5 in $.scores"), expected);
    }

    @Test
    void boolLiteralInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(true), Exp.listBin("flags"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("true in $.flags"), expected);
    }

    @Test
    void listLiteralInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(List.of(1, 2, 3)), Exp.listBin("listOfLists"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("[1, 2, 3] in $.listOfLists"), expected);
    }

    @Test
    void mapLiteralInBin() {
        TreeMap<Integer, String> map = new TreeMap<>();
        map.put(1, "a");
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(map), Exp.listBin("mapItems"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("{1: \"a\"} in $.mapItems"), expected);
    }

    @Test
    void binInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("itemType"), Exp.listBin("allowedItems"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.itemType in $.allowedItems"), expected);
    }

    @Test
    void nestedPathInListLiteral() {
        parseFilterExp(ExpressionContext.of(
                "$.rooms.room1.rates.rateType in [\"RACK_RATE\", \"DISCOUNT\"]"));
    }

    @Test
    void caseInsensitiveIn() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name IN [\"Bob\"]"), expected);
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name In [\"Bob\"]"), expected);
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name iN [\"Bob\"]"), expected);
    }

    @Test
    void inWithAndOperator() {
        Exp expected = Exp.and(
                Exp.gt(Exp.intBin("cost"), Exp.val(50)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("status"), Exp.val(List.of("active", "pending"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.cost > 50 and $.status in [\"active\", \"pending\"]"), expected);
    }

    @Test
    void inWithOrOperator() {
        Exp expected = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("status"), Exp.val(List.of("active"))),
                Exp.gt(Exp.intBin("priority"), Exp.val(5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.status in [\"active\"] or $.priority > 5"), expected);
    }

    @Test
    void complexExpressionWithIn() {
        Exp expected = Exp.and(
                Exp.gt(Exp.intBin("cost"), Exp.val(50)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("status"), Exp.listBin("allowedStatuses")),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.val("available"), Exp.listBin("bookableStates")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.cost > 50 and $.status in $.allowedStatuses" +
                        " and \"available\" in $.bookableStates"), expected);
    }

    @Test
    void inWithParentheses() {
        Exp expected = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.gt(Exp.intBin("age"), Exp.val(18)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("($.name in [\"Bob\"]) and $.age > 18"), expected);
    }

    @Test
    void inInsideWithStructure() {
        Exp expected = Exp.let(
                Exp.def("allowed", Exp.val(List.of("Bob", "Mary"))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.var("allowed")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("with(allowed = [\"Bob\", \"Mary\"])" +
                        " do ($.name.get(type: STRING) in ${allowed})"), expected);
    }

    @Test
    void inInsideWhenCondition() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.val("VIP"),
                Exp.val("regular"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.name.get(type: STRING) in [\"Bob\"] => \"VIP\"," +
                        " default => \"regular\")"), expected);
    }

    @Test
    void placeholderAsLeftOperand() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.val(List.of("gold", "silver")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [\"gold\", \"silver\"]",
                        PlaceholderValues.of("gold")), expected);
    }

    @Test
    void placeholderAsRightOperand() {
        parseFilterExp(ExpressionContext.of("$.name in ?0"));
    }

    @Test
    void inWithEmptyList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("name"), Exp.val(Collections.emptyList()));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name in []"), expected);
    }

    @Test
    void inWithSingleElementList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name in [\"Bob\"]"), expected);
    }

    @Test
    void inWithNegativeInts() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.val(List.of(-1, 0, 1)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.val in [-1, 0, 1]"), expected);
    }

    @Test
    void inWithNegativeFloats() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.val(List.of(-1.5, 0.0, 1.5)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.val in [-1.5, 0.0, 1.5]"), expected);
    }

    @Test
    void inWithHexBinaryLiterals() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.val(List.of(255, 5, 42)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.val in [0xFF, 0b101, 42]"), expected);
    }

    @Test
    void notWrappingIn() {
        Exp expected = Exp.not(ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("not($.name in [\"Bob\", \"Mary\"])"), expected);
    }

    @Test
    void arithmeticExprAsLeftIn() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.add(Exp.intBin("a"), Exp.val(5)),
                Exp.val(List.of(10, 20, 30)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a + 5 in [10, 20, 30]"), expected);
    }

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
