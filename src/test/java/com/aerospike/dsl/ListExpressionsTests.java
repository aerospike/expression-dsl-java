package com.aerospike.dsl;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.translate;
import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListExpressionsTests {

    @Test
    void listByIndexInteger() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        // Implicit detect as Int
        translateAndCompare("$.listBin1.[0] == 100", expected);
        translateAndCompare("$.listBin1.[0].get(type: INT) == 100", expected);
        translateAndCompare("$.listBin1.[0].get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.listBin1.[0].asInt() == 100", expected);
    }

    @Test
    void listByIndexOtherTypes() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val("stringVal"));
        // Implicit detect as string
        translateAndCompare("$.listBin1.[0] == \"stringVal\"", expected);
        translateAndCompare("$.listBin1.[0].get(type: STRING) == \"stringVal\"", expected);
        translateAndCompare("$.listBin1.[0].get(type: STRING, return: VALUE) == \"stringVal\"", expected);

        expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.BOOL,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(true));
        // Implicit detect as boolean
        translateAndCompare("$.listBin1.[0] == true", expected);
        translateAndCompare("$.listBin1.[0].get(type: BOOL) == true", expected);
        translateAndCompare("$.listBin1.[0].get(type: BOOL, return: VALUE) == true", expected);
    }

    @Test
    void listByValue() {
        Exp expected = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(100),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.listBin1.[=100] == 100", expected);
        translateAndCompare("$.listBin1.[=100].get(type: INT) == 100", expected);
        translateAndCompare("$.listBin1.[=100].get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.listBin1.[=100].asInt() == 100", expected);
    }

    @Test
    void listByValueCount() {
        Exp expected = Exp.gt(
                ListExp.getByValue(
                        ListReturnType.COUNT,
                        Exp.val(100),
                        Exp.listBin("listBin1")
                ),
                Exp.val(0));
        translateAndCompare("$.listBin1.[=100].count() > 0", expected);
    }

    @Test
    void listByRank() {
        Exp expected = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.listBin1.[#-1] == 100", expected);
        translateAndCompare("$.listBin1.[#-1].get(type: INT) == 100", expected);
        translateAndCompare("$.listBin1.[#-1].get(type: INT, return: VALUE) == 100", expected);
        translateAndCompare("$.listBin1.[#-1].asInt() == 100", expected);
    }

    @Test
    void listBinElementEquals_Nested() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(0),
                        CTX.listIndex(0)
                ),
                Exp.val(100));
        translateAndCompare("$.listBin1.[0].[0].[0] == 100", expected);
        translateAndCompare("$.listBin1.[0].[0].[0].get(type: INT) == 100", expected);
        translateAndCompare("$.listBin1.[0].[0].[0].get(type: INT, return: VALUE) == 100", expected);
    }

    @Test
    void listSize() {
        // Without Context
        Exp expected = Exp.eq(
                ListExp.size(Exp.listBin("listBin1")),
                Exp.val(1));
        translateAndCompare("$.listBin1.[].size() == 1", expected);

        // With Context
        expected = Exp.eq(
                ListExp.size(Exp.listBin("listBin1"), CTX.listIndex(2)),
                Exp.val(1));
        translateAndCompare("$.listBin1.[2].size() == 1", expected);
    }

    @Test
    void nestedLists() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(1),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5)
                ),
                Exp.val("stringVal"));
        translateAndCompare("$.listBin1.[5].[1].get(type: STRING) == \"stringVal\"", expected);
    }

    @Test
    void nestedListsWithDifferentContextTypes() {
        // Nested List Rank
        Exp expected = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(-1),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5)
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        translateAndCompare("$.listBin1.[5].[#-1] == \"stringVal\"", expected);
        translateAndCompare("$.listBin1.[5].[#-1].get(type: STRING) == \"stringVal\"", expected);

        // Nested List Rank Value
        expected = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(100),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5),
                        CTX.listRank(-1)
                ),
                Exp.val(200));
        // Implicit detect as Int
        translateAndCompare("$.listBin1.[5].[#-1].[=100] == 200", expected);
    }

//    @Test
//    void listBinElementCount() {
//        Exp expected = Exp.eq(
//                ListExp.getByIndex(
//                        ListReturnType.COUNT,
//                        Exp.Type.LIST, // TODO: how to determine the type of $.listBin1.[0]? could be list/map
//                        Exp.val(0),
//                        Exp.listBin("listBin1")
//                ),
//                Exp.val(100));
//        translateAndCompare("$.listBin1.[0].count() == 1", expected);
//        translateAndCompare("$.listBin1.[0].get(return: COUNT) == 1", expected);
//        translateAndCompare("$.listBin1.[0].get(type: INT, return: COUNT) == 1", expected);
//    }

    //@Test
    void negativeSyntaxList() {
        // TODO: should throw an exception (by ANTLR?)
        assertThatThrownBy(() -> translate("$.listBin1.size() == 1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Get size from a List: unexpected value 'INT'");

        // TODO: throw meaningful exception (by ANTLR?)
        assertThatThrownBy(() -> translate("$.listBin1.[stringValue] == 100"))
                .isInstanceOf(NullPointerException.class);
    }

    //@Test
    void negativeTypeComparisonList() {
        // TODO: should fail? Exp is successfully created but comparing int to a string value (validations on List)
        assertThatThrownBy(() -> translate("$.listBin1.[#-1].get(type: INT) == \"stringValue\""))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void listIndexRange() {
        Exp expected = ListExp.getByIndexRange(
                ListReturnType.VALUE,
                Exp.val(1),
                Exp.val(3),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[1:3]", expected);

        // Negative
        expected = ListExp.getByIndexRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.val(1),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[-3:1]", expected);

        // Inverted
        expected = ListExp.getByIndexRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(2),
                Exp.val(4),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[!2:4]", expected);

        // From start till the end
        expected = ListExp.getByIndexRange(
                ListReturnType.VALUE,
                Exp.val(1),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[1:]", expected);
    }

    @Test
    void listValueList() {
        Exp expected = ListExp.getByValueList(
                ListReturnType.VALUE,
                Exp.val(List.of("a", "b", "c")),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[=a,b,c]", expected);
        translateAndCompare("$.listBin1.[=\"a\",\"b\",\"c\"]", expected);

        // Integer
        expected = ListExp.getByValueList(
                ListReturnType.VALUE,
                Exp.val(List.of(1, 2, 3)),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[=1,2,3]", expected);

        // Inverted
        expected = ListExp.getByValueList(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(List.of("a", "b", "c")),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[!=a,b,c]", expected);
        translateAndCompare("$.listBin1.[!=\"a\",\"b\",\"c\"]", expected);
    }

    @Test
    void listValueRange() {
        Exp expected = ListExp.getByValueRange(
                ListReturnType.VALUE,
                Exp.val(111),
                Exp.val(334),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[=111:334]", expected);

        // Inverted
        expected = ListExp.getByValueRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(10),
                Exp.val(20),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[!=10:20]", expected);

        // From start till the end
        expected = ListExp.getByValueRange(
                ListReturnType.VALUE,
                Exp.val(111),
                null,
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[=111:]", expected);
    }

    @Test
    void listRankRange() {
        Exp expected = ListExp.getByRankRange(
                ListReturnType.VALUE,
                Exp.val(0),
                Exp.val(3),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[#0:3]", expected);

        // Inverted
        expected = ListExp.getByRankRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(0),
                Exp.val(3),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[!#0:3]", expected);

        // From start till the end
        expected = ListExp.getByRankRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.listBin("listBin1"));
        translateAndCompare("$.listBin1.[#-3:]", expected);

        // From start till the end with context
        expected = ListExp.getByRankRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.listBin("listBin1"),
                CTX.listIndex(5));
        translateAndCompare("$.listBin1.[5].[#-3:]", expected);
    }
}
