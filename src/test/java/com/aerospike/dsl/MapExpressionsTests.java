package com.aerospike.dsl;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;

public class MapExpressionsTests {

    @Test
    void mapOneLevelExpressions() {
        // Int
        Exp testExp = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.a == 200", testExp);
        translateAndCompare("$.mapBin1.a.get(type: INT) == 200", testExp);
        translateAndCompare("$.mapBin1.a.get(type: INT, return: VALUE) == 200", testExp);
        translateAndCompare("$.mapBin1.a.asInt() == 200", testExp);

        // String
        testExp = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val("stringVal"));
        //translateAndCompare("$.mapBin1.a == \"stringVal\"", testExp); // TODO: fix
        translateAndCompare("$.mapBin1.a.get(type: STRING) == \"stringVal\"", testExp);
        translateAndCompare("$.mapBin1.a.get(type: STRING, return: VALUE) == \"stringVal\"", testExp);
    }

    @Test
    void mapNestedLevelExpressions() {
        Exp testExp = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.a.bb.bcc > 200", testExp);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: INT) > 200", testExp);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > 200", testExp);
        //translateAndCompare("$.mapBin1.a.\"66\".get(type: STR) == \"ff\"", testExp);
        //translateAndCompare("$.mapBin1.a.dd.[1].{#0}.get(return: UNORDERED_MAP)", testExp);

        // String
        testExp = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val("stringVal"));
        //translateAndCompare("$.mapBin1.a.bb.bcc == \"stringVal\"", testExp); // TODO: fix
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: STRING) == \"stringVal\"", testExp);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE) == \"stringVal\"", testExp);
    }
}
