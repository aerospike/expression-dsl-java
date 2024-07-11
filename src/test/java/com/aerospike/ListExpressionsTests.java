package com.aerospike;

import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import static com.aerospike.TestUtils.translateAndCompare;

public class ListExpressionsTests {

    @Test
    void listBinElementEquals() {
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
//
//    @Test
//    void listBinElementEquals_Nested() {
//        translateAndCompare("$.listBin1.[0].[0].[0] == 100", testExp);
//        translateAndCompare("$.listBin1.[0].[0].[0].get(type: INT) == 100", testExp);
//        translateAndCompare("$.listBin1.[0].[0].[0].get(type: INT, return: VALUE) == 100", testExp);
//    }
//
    @Test
    void listBinSize() {
        Exp testExp = Exp.eq(
                ListExp.size(Exp.listBin("listBin1")),
                Exp.val(100));
        translateAndCompare("$.listBin1.[].size() == 1", testExp);
    }
//
//    @Test
//    void listBinElementCount() {
//        Exp testExp = Exp.eq(
//                ListExp.getByIndex(
//                        ListReturnType.COUNT,
//                        Exp.Type.LIST, // TODO: how to determine it?
//                        Exp.val(0),
//                        Exp.listBin("listBin1")
//                ),
//                Exp.val(100));
//        translateAndCompare("$.listBin1.[0].count() == 1", testExp);
//        translateAndCompare("$.listBin1.[0].get(return: COUNT) == 1", testExp);
//        translateAndCompare("$.listBin1.[0].get(type: INT, return: COUNT) == 1", testExp);
//    }
}
