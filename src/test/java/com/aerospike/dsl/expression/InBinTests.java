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

import java.util.TreeMap;

import static com.aerospike.dsl.util.TestUtils.parseFilterExpressionAndCompare;

class InBinTests {

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
                Exp.val(java.util.List.of(1, 2, 3)), Exp.listBin("listOfLists"));
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
    void binInNestedPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin"),
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("allowedNames"),
                        Exp.mapBin("rooms"),
                        CTX.mapKey(Value.get("config"))));
        parseFilterExpressionAndCompare(ExpressionContext.of(
                "$.intBin in $.rooms.config.allowedNames"), expected);
    }
}
