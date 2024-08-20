package com.aerospike.dsl;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;

public class MapExpressionsTests {

    @Test
    void mapOneLevelExpressions() {
        // Int
        Exp expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(200));
        // Implicit detect as Int
        translateAndCompare("$.mapBin1.a == 200", expected);
        translateAndCompare("$.mapBin1.a.get(type: INT) == 200", expected);
        translateAndCompare("$.mapBin1.a.get(type: INT, return: VALUE) == 200", expected);
        translateAndCompare("$.mapBin1.a.asInt() == 200", expected);

        // String
        expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        translateAndCompare("$.mapBin1.a == \"stringVal\"", expected);
        translateAndCompare("$.mapBin1.a.get(type: STRING) == \"stringVal\"", expected);
        translateAndCompare("$.mapBin1.a.get(type: STRING, return: VALUE) == \"stringVal\"", expected);
    }

    @Test
    void mapNestedLevelExpressions() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.a.bb.bcc > 200", expected);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: INT) > 200", expected);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > 200", expected);

        // String
        expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        translateAndCompare("$.mapBin1.a.bb.bcc == \"stringVal\"", expected);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: STRING) == \"stringVal\"", expected);
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE) == \"stringVal\"", expected);
    }

    @Test
    void quotedStringInExpressionPath() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.a.bb.bcc.get(type: INT) > 200", expected);
        translateAndCompare("$.mapBin1.a.\"bb\".bcc.get(type: INT) > 200", expected);
        translateAndCompare("$.mapBin1.a.'bb'.bcc.get(type: INT) > 200", expected);

        expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("127.0.0.1"))
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.\"127.0.0.1\".bcc.get(type: INT) > 200", expected);
        translateAndCompare("$.mapBin1.'127.0.0.1'.bcc.get(type: INT) > 200", expected);
    }

    @Test
    void mapSize() {
        // Without Context
        Exp expected = Exp.gt(
                MapExp.size(
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.size() > 200", expected);

        // With Context
        expected = Exp.gt(
                MapExp.size(
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a"))
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.a.size() > 200", expected);
    }

    @Test
    void mapByIndex() {
        Exp expected = Exp.eq(
                MapExp.getByIndex(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.mapBin1.{0} == 100", expected);
        translateAndCompare("$.mapBin1.{0}.get(type: INT) == 100", expected);
        translateAndCompare("$.mapBin1.{0}.get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.mapBin1.{0}.asInt() == 100", expected);
    }

    @Test
    void mapByValue() {
        Exp expected = Exp.eq(
                MapExp.getByValue(
                        MapReturnType.VALUE,
                        Exp.val(100),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.mapBin1.{=100} == 100", expected);
        translateAndCompare("$.mapBin1.{=100}.get(type: INT) == 100", expected);
        translateAndCompare("$.mapBin1.{=100}.get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.mapBin1.{=100}.asInt() == 100", expected);
    }

    @Test
    void mapByValueCount() {
        Exp expected = Exp.gt(
                MapExp.getByValue(
                        MapReturnType.COUNT,
                        Exp.val(100),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(0));
        translateAndCompare("$.mapBin1.{=100}.count() > 0", expected);
    }

    @Test
    void mapByRank() {
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.mapBin1.{#-1} == 100", expected);
        translateAndCompare("$.mapBin1.{#-1}.get(type: INT) == 100", expected);
        translateAndCompare("$.mapBin1.{#-1}.get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.mapBin1.{#-1}.asInt() == 100", expected);
    }

    @Test
    void mapByRankWithNesting() {
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a"))
                ),
                Exp.val(100));
        translateAndCompare("$.mapBin1.a.{#-1} == 100", expected);
        translateAndCompare("$.mapBin1.a.{#-1}.get(type: INT) == 100", expected);
        translateAndCompare("$.mapBin1.a.{#-1}.get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.mapBin1.a.{#-1}.asInt() == 100", expected);
    }

    @Test
    void nestedListsWithDifferentContextTypes() {
        // Nested List Rank
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(-1),
                        Exp.mapBin("mapBin1"),
                        CTX.mapIndex(5)
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        translateAndCompare("$.mapBin1.{5}.{#-1} == \"stringVal\"", expected);
        translateAndCompare("$.mapBin1.{5}.{#-1}.get(type: STRING) == \"stringVal\"", expected);

        // Nested List Rank Value
        expected = Exp.eq(
                MapExp.getByValue(
                        MapReturnType.VALUE,
                        Exp.val(100),
                        Exp.mapBin("mapBin1"),
                        CTX.mapIndex(5),
                        CTX.mapRank(-1)
                ),
                Exp.val(200));
        translateAndCompare("$.mapBin1.{5}.{#-1}.{=100} == 200", expected);
    }

    @Test
    void mapKeyRange() {
        Exp expected = MapExp.getByKeyRange(
                MapReturnType.VALUE,
                Exp.val("a"),
                Exp.val("c"),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{a-c}", expected);
        translateAndCompare("$.mapBin1.{\"a\"-\"c\"}", expected);

        // Inverted
        expected = MapExp.getByKeyRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("a"),
                Exp.val("c"),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!a-c}", expected);
        translateAndCompare("$.mapBin1.{!\"a\"-\"c\"}", expected);

        // From start till the end
        expected = MapExp.getByKeyRange(
                MapReturnType.VALUE,
                Exp.val("a"),
                null,
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{a-}", expected);
        translateAndCompare("$.mapBin1.{\"a\"-}", expected);
    }

    @Test
    void mapKeyList() {
        Exp expected = MapExp.getByKeyList(
                MapReturnType.VALUE,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{a,b,c}", expected);
        translateAndCompare("$.mapBin1.{\"a\",\"b\",\"c\"}", expected);

        // Inverted
        expected = MapExp.getByKeyList(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!a,b,c}", expected);
        translateAndCompare("$.mapBin1.{!\"a\",\"b\",\"c\"}", expected);
    }

    @Test
    void mapIndexRange() {
        Exp expected = MapExp.getByIndexRange(
                MapReturnType.VALUE,
                Exp.val(1),
                Exp.val(3),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{1:3}", expected);

        // Negative
        expected = MapExp.getByIndexRange(
                MapReturnType.VALUE,
                Exp.val(-3),
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{-3:1}", expected);

        // Inverted
        expected = MapExp.getByIndexRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(2),
                Exp.val(4),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!2:4}", expected);

        // From start till the end
        expected = MapExp.getByIndexRange(
                MapReturnType.VALUE,
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{1:}", expected);
    }

    @Test
    void mapValueList() {
        Exp expected = MapExp.getByValueList(
                MapReturnType.VALUE,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{=a,b,c}", expected);
        translateAndCompare("$.mapBin1.{=\"a\",\"b\",\"c\"}", expected);

        // Integer
        expected = MapExp.getByValueList(
                MapReturnType.VALUE,
                Exp.val(List.of(1, 2, 3)),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{=1,2,3}", expected);

        // Inverted
        expected = MapExp.getByValueList(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!=a,b,c}", expected);
        translateAndCompare("$.mapBin1.{!=\"a\",\"b\",\"c\"}", expected);
    }

    @Test
    void mapValueRange() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val(111),
                Exp.val(334),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{=111:334}", expected);

        // Inverted
        expected = MapExp.getByValueRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(10),
                Exp.val(20),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!=10:20}", expected);

        // From start till the end
        expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val(111),
                null,
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{=111:}", expected);
    }

    @Test
    void mapRankRange() {
        Exp expected = MapExp.getByRankRange(
                MapReturnType.VALUE,
                Exp.val(0),
                Exp.val(3),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{#0:3}", expected);

        // Inverted
        expected = MapExp.getByRankRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(0),
                Exp.val(3),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!#0:3}", expected);

        // From start till the end
        expected = MapExp.getByRankRange(
                MapReturnType.VALUE,
                Exp.val(-3),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{#-3:}", expected);

        // From start till the end with context
        expected = MapExp.getByRankRange(
                MapReturnType.VALUE,
                Exp.val(-3),
                Exp.mapBin("mapBin1"),
                CTX.mapIndex(5));
        translateAndCompare("$.mapBin1.{5}.{#-3:}", expected);
    }

    @Test
    void mapRankRangeRelative() {
        Exp expected = MapExp.getByValueRelativeRankRange(
                MapReturnType.VALUE,
                Exp.val(-1),
                Exp.val(10),
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{#-1:1~10}", expected);

        // Inverted
        expected = MapExp.getByValueRelativeRankRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(-1),
                Exp.val(10),
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{!#-1:1~10}", expected);

        // From start till the end
        expected = MapExp.getByValueRelativeRankRange(
                MapReturnType.VALUE,
                Exp.val(-2),
                Exp.val(10),
                Exp.mapBin("mapBin1"));
        translateAndCompare("$.mapBin1.{#-2:~10}", expected);
    }
}
