package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static com.aerospike.dsl.util.TestUtils.translate;
import static com.aerospike.dsl.util.TestUtils.translateAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Explicit types tests, list and map explicit types are tested in their own test classes
public class ExplicitTypesTests {

    @Test
    void integerComparison() {
        translateAndCompare("$.intBin1.get(type: INT) > 5",
                Exp.gt(Exp.intBin("intBin1"), Exp.val(5)));
    }

    @Test
    void stringComparison() {
        translateAndCompare("$.stringBin1.get(type: STRING) == \"yes\"",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.val("yes")));
    }

    @Test
    void blobComparison() {
        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        translateAndCompare("$.blobBin1.get(type: BLOB) == \"" + encodedString + "\"",
                Exp.eq(Exp.blobBin("blobBin1"), Exp.val(data)));

        // Reverse
        translateAndCompare("\"" + encodedString + "\"" + " == $.blobBin1.get(type: BLOB)",
                Exp.eq(Exp.val(data), Exp.blobBin("blobBin1")));
    }

    @Test
    void floatComparison() {
        translateAndCompare("$.floatBin1.get(type: FLOAT) == 1.5",
                Exp.eq(Exp.floatBin("floatBin1"), Exp.val(1.5)));
    }

    @Test
    void booleanComparison() {
        translateAndCompare("$.boolBin1.get(type: BOOL) == true",
                Exp.eq(Exp.boolBin("boolBin1"), Exp.val(true)));
    }

    @Test
    void negativeBooleanComparison() {
        assertThatThrownBy(() -> translate("$.boolBin1.get(type: BOOL) == 5"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare BOOL to INT");
    }

    @Test
    void twoStringBinsComparison() {
        translateAndCompare("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.stringBin("stringBin2")));
    }

    @Test
    void twoIntegerBinsComparison() {
        translateAndCompare("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)",
                Exp.eq(Exp.intBin("intBin1"), Exp.intBin("intBin2")));
    }

    @Test
    void twoFloatBinsComparison() {
        translateAndCompare("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)",
                Exp.eq(Exp.floatBin("floatBin1"), Exp.floatBin("floatBin2")));
    }

    @Test
    void twoBlobBinsComparison() {
        translateAndCompare("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)",
                Exp.eq(Exp.blobBin("blobBin1"), Exp.blobBin("blobBin2")));
    }

    @Test
    void negativeTwoDifferentBinTypesComparison() {
        assertThatThrownBy(() -> translate("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }

    @Test
    void secondDegreeExplicitFloat() {
        translateAndCompare("($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT)) > 10.5",
                Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)));
    }

    @Test
    void forthDegreeComplicatedExplicitFloat() {
        translateAndCompare("(($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT))" +
                        " + ($.oranges.get(type: FLOAT) + $.acai.get(type: FLOAT))) > 10.5",
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")),
                                Exp.add(Exp.floatBin("oranges"), Exp.floatBin("acai"))),
                        Exp.val(10.5))
        );
    }

    @Test
    void complicatedWhenExplicitTypeIntDefault() {
        Exp expectedExp = Exp.eq(
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

        translateAndCompare("$.a.get(type: INT) == " +
                        "(when($.b.get(type: INT) == 1 => $.a1.get(type: INT)," +
                        " $.b.get(type: INT) == 2 => $.a2.get(type: INT)," +
                        " $.b.get(type: INT) == 3 => $.a3.get(type: INT)," +
                        " default => $.a4.get(type: INT) + 1))",
                expectedExp);
    }

    // TODO: get function with type doesn't work in this case - investigate
    // @Test
    void complicatedWhenExplicitTypeString() {
        Exp expectedExp = Exp.eq(
                Exp.stringBin("a"),
                Exp.cond(
                        Exp.eq(
                                Exp.stringBin("b"),
                                Exp.val(1)
                        ), Exp.stringBin("a1"),
                        Exp.eq(
                                Exp.stringBin("b"),
                                Exp.val(2)
                        ), Exp.stringBin("a2"),
                        Exp.eq(
                                Exp.stringBin("b"),
                                Exp.val(3)
                        ), Exp.stringBin("a3"),
                        Exp.val("hello")
                )
        );

        translateAndCompare("$.a.get(type: STRING) == " +
                        "(when($.b.get(type: STRING) == 1 => $.a1.get(type: STRING)," +
                        " $.b.get(type: STRING) == 2 => $.a2.get(type: STRING)," +
                        " $.b.get(type: STRING) == 3 => $.a3.get(type: STRING)," +
                        " default => \"hello\")",
                expectedExp);
    }
}
