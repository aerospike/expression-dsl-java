package com.aerospike.dsl.expression;

import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.Value;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.cdt.MapReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import com.aerospike.dsl.client.exp.MapExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilterExpressionAndCompare;

class InGrammarConflictTests {

    @Test
    void caseInsensitiveIn() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name IN [\"Bob\"]"), expected);
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name In [\"Bob\"]"), expected);
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name iN [\"Bob\"]"), expected);
    }

    @Test
    void binNamedInEquality() {
        Exp expected = Exp.eq(Exp.intBin("in"), Exp.val(5));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.in == 5"), expected);
    }

    @Test
    void binNamedInInList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("in"), Exp.val(List.of(1, 2)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.in in [1, 2]"), expected);
    }

    @Test
    void mapKeyNamedIn() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("in"),
                        Exp.mapBin("map"),
                        CTX.mapKey(Value.get("a"))),
                Exp.val(5));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.map.a.in > 5"), expected);
    }

    @Test
    void simpleMapKeyNamedIn() {
        Exp expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("in"),
                        Exp.mapBin("list")),
                Exp.val(5));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.list.in == 5"), expected);
    }

    @Test
    void listValueNamedIn() {
        Exp expected = Exp.eq(
                ListExp.getByValue(ListReturnType.VALUE,
                        Exp.val("in"), Exp.listBin("listBin")),
                Exp.val("hello"));
        parseFilterExpressionAndCompare(ExpressionContext.of(
                "$.listBin.[=in].get(type: STRING) == \"hello\""), expected);
    }

    @Test
    void listValueNamedInUpperCase() {
        Exp expected = Exp.eq(
                ListExp.getByValue(ListReturnType.VALUE,
                        Exp.val("IN"), Exp.listBin("listBin")),
                Exp.val("hello"));
        parseFilterExpressionAndCompare(ExpressionContext.of(
                "$.listBin.[=IN].get(type: STRING) == \"hello\""), expected);
    }

    @Test
    void mapValueNamedIn() {
        Exp expected = Exp.eq(
                MapExp.getByValue(MapReturnType.VALUE,
                        Exp.val("in"), Exp.mapBin("mapBin")),
                Exp.val("hello"));
        parseFilterExpressionAndCompare(ExpressionContext.of(
                "$.mapBin.{=in}.get(type: STRING) == \"hello\""), expected);
    }

    @Test
    void mapValueNamedInUpperCase() {
        Exp expected = Exp.eq(
                MapExp.getByValue(MapReturnType.VALUE,
                        Exp.val("IN"), Exp.mapBin("mapBin")),
                Exp.val("hello"));
        parseFilterExpressionAndCompare(ExpressionContext.of(
                "$.mapBin.{=IN}.get(type: STRING) == \"hello\""), expected);
    }

    @Test
    void mapKeyRangeStartIn() {
        Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                Exp.val("in"), Exp.val("z"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{in-z}"), expected);
    }

    @Test
    void mapKeyRangeEndIn() {
        Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                Exp.val("a"), Exp.val("in"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{a-in}"), expected);
    }

    @Test
    void mapKeyRangeOpenEndIn() {
        Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                Exp.val("in"), null, Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{in-}"), expected);
    }

    @Test
    void invertedKeyRangeStartIn() {
        Exp expected = MapExp.getByKeyRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("in"), Exp.val("z"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!in-z}"), expected);
    }

    @Test
    void invertedKeyRangeEndIn() {
        Exp expected = MapExp.getByKeyRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("a"), Exp.val("in"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!a-in}"), expected);
    }

    @Test
    void mapKeyListWithIn() {
        Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                Exp.val(List.of("in", "z")), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{in,z}"), expected);
    }

    @Test
    void mapKeyListOnlyIn() {
        Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                Exp.val(List.of("in")), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{in}"), expected);
    }

    @Test
    void invertedKeyListWithIn() {
        Exp expected = MapExp.getByKeyList(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(List.of("in", "z")), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!in,z}"), expected);
    }

    @Test
    void relativeIndexWithKeyIn() {
        Exp expected = MapExp.getByKeyRelativeIndexRange(MapReturnType.VALUE,
                Exp.val("in"), Exp.val(0), Exp.val(1), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{0:1~in}"), expected);
    }

    @Test
    void invertedRelativeIndexKeyIn() {
        Exp expected = MapExp.getByKeyRelativeIndexRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("in"), Exp.val(0), Exp.val(1), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!0:1~in}"), expected);
    }

    @Test
    void mapKeyRangeStartInUpperCase() {
        Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                Exp.val("IN"), Exp.val("z"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{IN-z}"), expected);
    }

    @Test
    void mapKeyRangeEndInUpperCase() {
        Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                Exp.val("a"), Exp.val("IN"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{a-IN}"), expected);
    }

    @Test
    void mapKeyRangeOpenEndInUC() {
        Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                Exp.val("IN"), null, Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{IN-}"), expected);
    }

    @Test
    void invertedKeyRangeInUpperCase() {
        Exp expected = MapExp.getByKeyRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("IN"), Exp.val("z"), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!IN-z}"), expected);
    }

    @Test
    void mapKeyListWithInUpperCase() {
        Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                Exp.val(List.of("IN", "z")), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{IN,z}"), expected);
    }

    @Test
    void invertedKeyListInUpperCase() {
        Exp expected = MapExp.getByKeyList(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(List.of("IN", "z")), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!IN,z}"), expected);
    }

    @Test
    void relativeIndexKeyInUpperCase() {
        Exp expected = MapExp.getByKeyRelativeIndexRange(MapReturnType.VALUE,
                Exp.val("IN"), Exp.val(0), Exp.val(1), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{0:1~IN}"), expected);
    }

    @Test
    void invertedRelativeKeyInUC() {
        Exp expected = MapExp.getByKeyRelativeIndexRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("IN"), Exp.val(0), Exp.val(1), Exp.mapBin("mapBin"));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.{!0:1~IN}"), expected);
    }
}
