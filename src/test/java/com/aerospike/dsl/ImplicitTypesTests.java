package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.TestUtils.translateAndCompare;

public class ImplicitTypesTests {

    @Test
    void floatComparison() {
        translateAndCompare("$.floatBin1 >= 100.25",
                Exp.ge(Exp.floatBin("floatBin1"), Exp.val(100.25)));
    }

    @Test
    void booleanComparison() {
        translateAndCompare("$.boolBin1 == true",
                Exp.eq(Exp.boolBin("boolBin1"), Exp.val(true)));
        translateAndCompare("false == $.boolBin1",
                Exp.eq(Exp.val(false), Exp.boolBin("boolBin1")));
        translateAndCompare("$.boolBin1 != false",
                Exp.ne(Exp.boolBin("boolBin1"), Exp.val(false)));
    }

    // Logical expressions are always treated on boolean operands
    // this can also be an expression that evaluates to a boolean result
    @Test
    void binBooleanImplicitLogicalComparison() {
        translateAndCompare("$.boolBin1 and $.boolBin2",
                Exp.and(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
        translateAndCompare("$.boolBin1 or $.boolBin2",
                Exp.or(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
        translateAndCompare("not($.boolBin1)",
                Exp.not(Exp.boolBin("boolBin1")));
        translateAndCompare("exclusive($.boolBin1, $.boolBin2)",
                Exp.exclusive(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
    }

    @Test
    void implicitDefaultIntComparison() {
        translateAndCompare("$.intBin1 < $.intBin2",
                Exp.lt(Exp.intBin("intBin1"), Exp.intBin("intBin2")));
    }

    // A workaround for this is to use explicit types
    //@Test
    void secondDegreeImplicitCastingFloat() {
        // TODO: Need to solve this case, we can't identify by "2nd or more degree operand",
        //  at the moment will be compared as integers!
        translateAndCompare("($.apples + $.bananas) > 10.5",
                Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)));
    }
}
