package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;

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

    @Test
    void secondDegreeImplicitCastingFloat() {
        translateAndCompare("($.apples + $.bananas) > 10.5",
                Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)));
    }

    @Test
    void secondDegreeComplicatedFloatFirstImplicitCastingFloat() {
        translateAndCompare("($.apples + $.bananas) > 10.5 and ($.oranges + $.grapes) <= 5",
                Exp.and(
                        Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)),
                        Exp.le(Exp.add(Exp.intBin("oranges"), Exp.intBin("grapes")), Exp.val(5)))
        );
    }

    @Test
    void secondDegreeComplicatedIntFirstImplicitCastingFloat() {
        translateAndCompare("($.apples + $.bananas) > 5 and ($.oranges + $.grapes) <= 10.5",
                Exp.and(
                        Exp.gt(Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(5)),
                        Exp.le(Exp.add(Exp.floatBin("oranges"), Exp.floatBin("grapes")), Exp.val(10.5)))
        );
    }

    @Test
    void thirdDegreeComplicatedDefaultInt() {
        translateAndCompare("(($.apples + $.bananas) + $.oranges) > 10",
                Exp.gt(
                        Exp.add(Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.intBin("oranges")),
                        Exp.val(10))
        );
    }

    @Test
    void thirdDegreeComplicatedImplicitCastingFloat() {
        translateAndCompare("(($.apples + $.bananas) + $.oranges) > 10.5",
                Exp.gt(
                        Exp.add(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.floatBin("oranges")),
                        Exp.val(10.5))
        );
    }

    @Test
    void forthDegreeComplicatedDefaultInt() {
        translateAndCompare("(($.apples + $.bananas) + ($.oranges + $.acai)) > 10",
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")),
                                Exp.add(Exp.intBin("oranges"), Exp.intBin("acai"))),
                        Exp.val(10))
        );
    }

    @Test
    void forthDegreeComplicatedImplicitCastingFloat() {
        translateAndCompare("(($.apples + $.bananas) + ($.oranges + $.acai)) > 10.5",
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")),
                                Exp.add(Exp.floatBin("oranges"), Exp.floatBin("acai"))),
                        Exp.val(10.5))
        );
    }

    @Test
    void complicatedWhenImplicitTypeInt() {
        Exp expected = Exp.eq(
                Exp.intBin("a"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(1)
                        ), Exp.intBin("a1"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(2)
                        ), Exp.intBin("a2"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(3)
                        ), Exp.intBin("a3"),
                        Exp.add(Exp.intBin("a4"), Exp.val(1))
                )
        );

        translateAndCompare("$.a == (when($.b == 1 => $.a1, $.b == 2 => $.a2, $.b == 3 => $.a3, default => $.a4+1))",
                expected);
    }

    // TODO: FMWK-533 Implicit Type Detection for Control Structures
    //@Test
    void complicatedWhenImplicitTypeString() {
        Exp expected = Exp.eq(
                Exp.stringBin("a"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(1)
                        ), Exp.stringBin("a1"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(2)
                        ), Exp.stringBin("a2"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(3)
                        ), Exp.stringBin("a3"),
                        Exp.val("hello")
                )
        );

        translateAndCompare("$.a == (when($.b == 1 => $.a1, $.b == 2 => $.a2, $.b == 3 => $.a3, default => \"hello\"))",
                expected);
    }
}
