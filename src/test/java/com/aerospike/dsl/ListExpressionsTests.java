package com.aerospike.dsl;

import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.TestUtils.translateAndCompare;

public class ListExpressionsTests {

    @Test
    void listBinElementEquals_ByIndex() {
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
}
