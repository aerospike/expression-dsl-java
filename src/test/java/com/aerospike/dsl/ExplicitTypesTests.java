package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static com.aerospike.dsl.util.TestUtils.translateAndCompare;

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

    // This test passes because creating an invalid Aerospike Exp is legal, executing it will result in exception
    // From Aerospike database. To avoid it we need to add validation at the DSL level.
    @Test
    void twoDifferentBinsComparison() {
        translateAndCompare("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.floatBin("floatBin2")));
    }

    @Test
    void secondDegreeExplicitCasting() {
        translateAndCompare("($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT)) > 10.5",
                Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)));
    }
}
