package com.aerospike.dsl;

import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translate;
import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListExpressionsTests {

    @Test
    void listBinElementIntEquals_ByIndex() {
        Exp testExp = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.listBin1.[0] == 100", testExp);
        translateAndCompare("$.listBin1.[0].get(type: INT) == 100", testExp);
        translateAndCompare("$.listBin1.[0].get(type: INT, return: VALUE) == 100", testExp);
        translateAndCompare("$.listBin1.[0].asInt() == 100", testExp);
    }

    @Test
    void listBinElementOtherTypesEquals_ByIndex() {
        Exp testExp = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val("stringVal"));
        translateAndCompare("$.listBin1.[0].get(type: STRING) == \"stringVal\"", testExp);
        translateAndCompare("$.listBin1.[0].get(type: STRING, return: VALUE) == \"stringVal\"", testExp);

        testExp = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.BOOL,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(true));
        translateAndCompare("$.listBin1.[0].get(type: BOOL) == true", testExp);
        translateAndCompare("$.listBin1.[0].get(type: BOOL, return: VALUE) == true", testExp);
    }

    @Test
    void listBinElementEquals_ByValue() {
        Exp testExp = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(100),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.listBin1.[=100] == 100", testExp);
        translateAndCompare("$.listBin1.[=100].get(type: INT) == 100", testExp);
        translateAndCompare("$.listBin1.[=100].get(type: INT, return: VALUE) == 100", testExp);
        translateAndCompare("$.listBin1.[=100].asInt() == 100", testExp);
    }

    @Test
    void listBinElementEquals_ByValue_Count() {
        Exp testExp = Exp.gt(
                ListExp.getByValue(
                        ListReturnType.COUNT,
                        Exp.val(100),
                        Exp.listBin("listBin1")
                ),
                Exp.val(0));
        translateAndCompare("$.listBin1.[=100].count() > 0", testExp);
    }

    @Test
    void listBinElementEquals_ByRank() {
        Exp testExp = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        translateAndCompare("$.listBin1.[#-1] == 100", testExp);
        translateAndCompare("$.listBin1.[#-1].get(type: INT) == 100", testExp);
        translateAndCompare("$.listBin1.[#-1].get(type: INT, return: VALUE) == 100", testExp);
        translateAndCompare("$.listBin1.[#-1].asInt() == 100", testExp);
    }

    // Will be handled within context support task
//    @Test
//    void listBinElementEquals_Nested() {
//        Exp testExp = Exp.eq(
//                ListExp.getByIndex(
//                        ListReturnType.VALUE,
//                        Exp.Type.INT,
//                        Exp.val(0),
//                        Exp.listBin("listBin1")
//                ),
//                Exp.val(100));
//        translateAndCompare("$.listBin1.[0].[0].[0] == 100", testExp);
//        translateAndCompare("$.listBin1.[0].[0].[0].get(type: INT) == 100", testExp);
//        translateAndCompare("$.listBin1.[0].[0].[0].get(type: INT, return: VALUE) == 100", testExp);
//    }

    @Test
    void listBinSize() {
        Exp testExp = Exp.eq(
                ListExp.size(Exp.listBin("listBin1")),
                Exp.val(1));
        translateAndCompare("$.listBin1.[].size() == 1", testExp);
    }

//
//    @Test
//    void listBinElementCount() {
//        Exp testExp = Exp.eq(
//                ListExp.getByIndex(
//                        ListReturnType.COUNT,
//                        Exp.Type.LIST, // TODO: how to determine the type of $.listBin1.[0]?
//                        Exp.val(0),
//                        Exp.listBin("listBin1")
//                ),
//                Exp.val(100));
//        translateAndCompare("$.listBin1.[0].count() == 1", testExp);
//        translateAndCompare("$.listBin1.[0].get(return: COUNT) == 1", testExp);
//        translateAndCompare("$.listBin1.[0].get(type: INT, return: COUNT) == 1", testExp);
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
}
