package com.aerospike.dsl.expression;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseDslExpressionAndCompare;

public class MapAndListExpressionsTests {

    @Test
    void listInsideAMap() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a"))
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.mapBin1.a.[0] == 100", expected);

        expected = Exp.gt(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(2),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")),
                        CTX.mapKey(Value.get("cc"))
                ), Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.mapBin1.a.cc.[2].get(type: INT) > 100", expected);
    }

    @Test
    void mapListList() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")),
                        CTX.listIndex(0)
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.mapBin1.a.[0].[0] == 100", expected);
    }

    @Test
    void mapInsideAList() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("cc"),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(2)
                ), Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.listBin1.[2].cc.get(type: INT) > 100", expected);
    }

    @Test
    void listMapMap() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("cc"),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(2),
                        CTX.mapKey(Value.get("aa"))
                ), Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.listBin1.[2].aa.cc > 100", expected);
        TestUtils.parseFilterExpressionAndCompare("$.listBin1.[2].aa.cc.get(type: INT) > 100", expected);
    }

    @Test
    void listMapList() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(1),
                        CTX.mapKey(Value.get("a"))
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.listBin1.[1].a.[0] == 100", expected);
    }

    @Test
    void listMapListSize() {
        Exp expected = Exp.eq(
                ListExp.size(
                        ListExp.getByIndex(
                                ListReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val(0),
                                Exp.listBin("listBin1"),
                                CTX.listIndex(1),
                                CTX.mapKey(Value.get("a"))
                        )
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.listBin1.[1].a.[0].count() == 100", expected);
        TestUtils.parseFilterExpressionAndCompare("$.listBin1.[1].a.[0].[].count() == 100", expected);
    }

    @Test
    void mapListMap() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("cc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")),
                        CTX.listIndex(0)
                ), Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare("$.mapBin1.a.[0].cc > 100", expected);
        TestUtils.parseFilterExpressionAndCompare("$.mapBin1.a.[0].cc.get(type: INT) > 100", expected);
    }

    //@Test
    void mapAndListCombinations() {
        Exp expected = Exp.gt(
                ListExp.size(
                        MapExp.getByKey(
                                MapReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val("shape"),
                                Exp.mapBin("mapBin1")
                                //CTX.mapKey(Value.get("shape"))
                        )
                ),
                Exp.val(2));
        //translateAndCompare("$.mapBin1.shape.[].size() > 2", expected);
        //translateAndCompare("$.mapBin1.a.dd.[1].{#0}.get(return: UNORDERED_MAP)", expected);
    }
}
