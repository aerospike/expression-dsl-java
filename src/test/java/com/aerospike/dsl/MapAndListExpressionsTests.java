package com.aerospike.dsl;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;

public class MapAndListExpressionsTests {

    @Test
    void mapAndListComb() {
        Exp testExp = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a"))
                ),
                Exp.val(100));
        translateAndCompare("$.mapBin1.a.[0] == 100", testExp);

        testExp = Exp.gt(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(2),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")),
                        CTX.mapKey(Value.get("cc"))
                ), Exp.val(100));
        translateAndCompare("$.mapBin1.a.cc.[2].get(type: INT) > 100", testExp);
    }

    //@Test
    void listAndThenMap() {
        // TODO: verify expected expression is correct
        Exp testExp = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("cc"),
                        ListExp.getByIndex(
                                ListReturnType.VALUE,
                                Exp.Type.MAP,
                                Exp.val(2),
                                Exp.listBin("listBin1")
                        )
                ), Exp.val(100));
        translateAndCompare("$.listBin1.[2].cc.get(type: INT) > 100", testExp);
    }

    //@Test
    void mapAndListCombinations() {
        Exp testExp = Exp.gt(
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
        // translateAndCompare("$.mapBin1.shape.[].size() > 2", testExp);
        //translateAndCompare("$.mapBin1.a.dd.[1].{#0}.get(return: UNORDERED_MAP)", testExp);
    }
}
