package com.aerospike.dsl.expression;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.TreeMap;

import static com.aerospike.dsl.util.TestUtils.parseExpression;
import static com.aerospike.dsl.util.TestUtils.parseExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Explicit types tests, list and map explicit types are tested in their own test classes
public class ExplicitTypesTests {

    @Test
    void integerComparison() {
        parseExpressionAndCompare("$.intBin1.get(type: INT) > 5",
                Exp.gt(Exp.intBin("intBin1"), Exp.val(5)));

        parseExpressionAndCompare("5 < $.intBin1.get(type: INT)",
                Exp.lt(Exp.val(5), Exp.intBin("intBin1")));
    }

    @Test
    void stringComparison() {
        // A String constant must contain quoted Strings
        parseExpressionAndCompare("$.stringBin1.get(type: STRING) == \"yes\"",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.val("yes")));

        parseExpressionAndCompare("$.stringBin1.get(type: STRING) == 'yes'",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.val("yes")));

        parseExpressionAndCompare("\"yes\" == $.stringBin1.get(type: STRING)",
                Exp.eq(Exp.val("yes"), Exp.stringBin("stringBin1")));

        parseExpressionAndCompare("'yes' == $.stringBin1.get(type: STRING)",
                Exp.eq(Exp.val("yes"), Exp.stringBin("stringBin1")));
    }

    @Test
    void stringComparisonNegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() -> parseExpressionAndCompare("$.stringBin1.get(type: STRING) == yes",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.val("yes"))))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse right operand");
    }

    @Test
    void blobComparison() {
        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        parseExpressionAndCompare("$.blobBin1.get(type: BLOB) == \"" + encodedString + "\"",
                Exp.eq(Exp.blobBin("blobBin1"), Exp.val(data)));

        // Reverse
        parseExpressionAndCompare("\"" + encodedString + "\"" + " == $.blobBin1.get(type: BLOB)",
                Exp.eq(Exp.val(data), Exp.blobBin("blobBin1")));
    }

    @Test
    void floatComparison() {
        parseExpressionAndCompare("$.floatBin1.get(type: FLOAT) == 1.5",
                Exp.eq(Exp.floatBin("floatBin1"), Exp.val(1.5)));

        parseExpressionAndCompare("1.5 == $.floatBin1.get(type: FLOAT)",
                Exp.eq(Exp.val(1.5), Exp.floatBin("floatBin1")));
    }

    @Test
    void booleanComparison() {
        parseExpressionAndCompare("$.boolBin1.get(type: BOOL) == true",
                Exp.eq(Exp.boolBin("boolBin1"), Exp.val(true)));

        parseExpressionAndCompare("true == $.boolBin1.get(type: BOOL)",
                Exp.eq(Exp.val(true), Exp.boolBin("boolBin1")));
    }

    @Test
    void negativeBooleanComparison() {
        assertThatThrownBy(() -> parseExpression("$.boolBin1.get(type: BOOL) == 5"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare BOOL to INT");
    }

    @Test
    void listComparison_constantOnRightSide() {
        parseExpressionAndCompare("$.listBin1.get(type: LIST) == [100]",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of(100))));

        parseExpressionAndCompare("$.listBin1.[] == [100]",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of(100))));

        // integer values are read as long
        parseExpressionAndCompare("$.listBin1.get(type: LIST) == [100, 200, 300, 400]",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of(100, 200, 300, 400))));

        // integer values are read as long
        parseExpressionAndCompare("$.listBin1.get(type: LIST) == [100, 200, 300, 400]",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of(100L, 200L, 300L, 400L))));

        parseExpressionAndCompare("$.listBin1.get(type: LIST) == ['yes']",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of("yes"))));

        parseExpressionAndCompare("$.listBin1.get(type: LIST) == ['yes', 'of course']",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of("yes", "of course"))));

        parseExpressionAndCompare("$.listBin1.get(type: LIST) == [\"yes\"]",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of("yes"))));

        parseExpressionAndCompare("$.listBin1.get(type: LIST) == [\"yes\", \"of course\"]",
                Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of("yes", "of course"))));
    }

    @Test
    void listComparison_constantOnRightSide_NegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() ->
                parseExpressionAndCompare("$.listBin1.get(type: LIST) == [yes, of course]",
                        Exp.eq(Exp.listBin("listBin1"), Exp.val(List.of("yes", "of course"))))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse list operand");
    }

    @Test
    void listComparison_constantOnLeftSide() {
        parseExpressionAndCompare("[100] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of(100)), Exp.listBin("listBin1")));

        parseExpressionAndCompare("[100] == $.listBin1.[]",
                Exp.eq(Exp.val(List.of(100)), Exp.listBin("listBin1")));

        // integer values are read as long
        parseExpressionAndCompare("[100, 200, 300, 400] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of(100, 200, 300, 400)), Exp.listBin("listBin1")));

        // integer values are read as long
        parseExpressionAndCompare("[100, 200, 300, 400] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of(100L, 200L, 300L, 400L)), Exp.listBin("listBin1")));

        parseExpressionAndCompare("['yes'] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of("yes")), Exp.listBin("listBin1")));

        parseExpressionAndCompare("['yes', 'of course'] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of("yes", "of course")), Exp.listBin("listBin1")));

        parseExpressionAndCompare("[\"yes\"] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of("yes")), Exp.listBin("listBin1")));

        parseExpressionAndCompare("[\"yes\", \"of course\"] == $.listBin1.get(type: LIST)",
                Exp.eq(Exp.val(List.of("yes", "of course")), Exp.listBin("listBin1")));
    }

    @Test
    void listComparison_constantOnLeftSide_NegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() ->
                parseExpressionAndCompare("[yes, of course] == $.listBin1.get(type: LIST)",
                        Exp.eq(Exp.val(List.of("yes", "of course")), Exp.listBin("listBin1")))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");
    }

    @SuppressWarnings("unchecked")
    public static <K, V> TreeMap<K, V> treeMapOf(Object... entries) {
        TreeMap<K, V> map = new TreeMap<>();

        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException("You must provide an even number of arguments.");
        }

        for (int i = 0; i < entries.length; i += 2) {
            K key = (K) entries[i];
            V value = (V) entries[i + 1];
            map.put(key, value);
        }
        return map;
    }

    @Test
    void mapComparison_constantOnRightSide() {
        // Prerequisite for comparing maps: both sides must be ordered maps
        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {100:100}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf(100, 100))));

        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {100 : 100}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf(100, 100))));

        parseExpressionAndCompare("$.mapBin1.{} == {100:100}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf(100, 100))));

        byte[] blobKey = new byte[]{1, 2, 3};
        String encodedBlobKey = Base64.getEncoder().encodeToString(blobKey);
        // encoded blob key must be quoted as it is a String
        parseExpressionAndCompare("$.mapBin1.{} == {'" + encodedBlobKey + "':100}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf(encodedBlobKey, 100))));

        // integer values are read as long
        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {100:200, 300:400}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf(100L, 200L, 300L, 400L))));

        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {100:200, 300:400}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf(100, 200, 300, 400))));

        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {'yes?':'yes!'}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf("yes?", "yes!"))));

        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {\"yes\" : \"yes\"}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf("yes", "yes"))));

        parseExpressionAndCompare(
                "$.mapBin1.get(type: MAP) == {\"yes of course\" : \"yes of course\"}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf("yes of course", "yes of course"))));

        parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {\"yes\" : [\"yes\", \"of course\"]}",
                Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf("yes", List.of("yes",  "of course")))));
    }

    @Test
    void mapComparison_constantOnRightSide_NegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() ->
                parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {yes, of course}",
                        Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf("yes", "of course"))))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse map operand");

        assertThatThrownBy(() ->
                parseExpressionAndCompare("$.mapBin1.get(type: MAP) == ['yes', 'of course']",
                        Exp.eq(Exp.mapBin("mapBin1"), Exp.val(List.of("yes", "of course"))))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Cannot compare MAP to LIST");

        // Map key can only be Integer or String
        assertThatThrownBy(() ->
                parseExpressionAndCompare("$.mapBin1.get(type: MAP) == {[100]:[100]}",
                        Exp.eq(Exp.mapBin("mapBin1"), Exp.val(List.of("yes", "of course"))))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse map operand");
    }

    @Test
    void mapComparison_constantOnLeftSide() {
        // Prerequisite for comparing maps: both sides must be ordered maps
        parseExpressionAndCompare("{100:100} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf(100, 100)), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare("{100 : 100} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf(100, 100)), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare("{100:100} == $.mapBin1.{}",
                Exp.eq(Exp.val(treeMapOf(100, 100)), Exp.mapBin("mapBin1")));

        byte[] blobKey = new byte[]{1, 2, 3};
        String encodedBlobKey = Base64.getEncoder().encodeToString(blobKey);
        // encoded blob key must be quoted as it is a String
        parseExpressionAndCompare("{'" + encodedBlobKey + "':100} == $.mapBin1.{}",
                Exp.eq(Exp.val(treeMapOf(encodedBlobKey, 100)), Exp.mapBin("mapBin1")));

        // integer values are read as long
        parseExpressionAndCompare("{100:200, 300:400} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf(100L, 200L, 300L, 400L)), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare("{100:200, 300:400} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf(100, 200, 300, 400)), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare("{'yes?':'yes!'} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf("yes?", "yes!")), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare("{\"yes\" : \"yes\"} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf("yes", "yes")), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare(
                "{\"yes of course\" : \"yes of course\"} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf("yes of course", "yes of course")), Exp.mapBin("mapBin1")));

        parseExpressionAndCompare("{\"yes\" : [\"yes\", \"of course\"]} == $.mapBin1.get(type: MAP)",
                Exp.eq(Exp.val(treeMapOf("yes", List.of("yes",  "of course"))), Exp.mapBin("mapBin1")));
    }

    @Test
    void mapComparison_constantOnLeftSide_NegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() ->
                parseExpressionAndCompare("{yes, of course} == $.mapBin1.get(type: MAP)",
                        Exp.eq(Exp.mapBin("mapBin1"), Exp.val(treeMapOf("of course", "yes"))))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");

        assertThatThrownBy(() ->
                parseExpressionAndCompare("['yes', 'of course'] == $.mapBin1.get(type: MAP)", // incorrect: must be {}
                        Exp.eq(Exp.val(List.of("yes", "of course")), Exp.mapBin("mapBin1")))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Cannot compare LIST to MAP");

        // Map key can only be Integer or String
        assertThatThrownBy(() ->
                parseExpressionAndCompare("{[100]:[100]} == $.mapBin1.get(type: MAP)",
                        Exp.eq(Exp.val(List.of("yes", "of course")), Exp.mapBin("mapBin1")))
        )
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");
    }

    @Test
    void twoStringBinsComparison() {
        parseExpressionAndCompare("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)",
                Exp.eq(Exp.stringBin("stringBin1"), Exp.stringBin("stringBin2")));
    }

    @Test
    void twoIntegerBinsComparison() {
        parseExpressionAndCompare("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)",
                Exp.eq(Exp.intBin("intBin1"), Exp.intBin("intBin2")));
    }

    @Test
    void twoFloatBinsComparison() {
        parseExpressionAndCompare("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)",
                Exp.eq(Exp.floatBin("floatBin1"), Exp.floatBin("floatBin2")));
    }

    @Test
    void twoBlobBinsComparison() {
        parseExpressionAndCompare("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)",
                Exp.eq(Exp.blobBin("blobBin1"), Exp.blobBin("blobBin2")));
    }

    @Test
    void negativeTwoDifferentBinTypesComparison() {
        assertThatThrownBy(() -> parseExpression("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }

    @Test
    void secondDegreeExplicitFloat() {
        parseExpressionAndCompare("($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT)) > 10.5",
                Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)));
    }

    @Test
    void forthDegreeComplicatedExplicitFloat() {
        parseExpressionAndCompare("(($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT))" +
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

        parseExpressionAndCompare("$.a.get(type: INT) == " +
                        "(when($.b.get(type: INT) == 1 => $.a1.get(type: INT)," +
                        " $.b.get(type: INT) == 2 => $.a2.get(type: INT)," +
                        " $.b.get(type: INT) == 3 => $.a3.get(type: INT)," +
                        " default => $.a4.get(type: INT) + 1))",
                expected);
    }

    @Test
    void complicatedWhenExplicitTypeString() {
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

        parseExpressionAndCompare("$.a.get(type: STRING) == " +
                        "(when($.b == 1 => $.a1.get(type: STRING)," +
                        " $.b == 2 => $.a2.get(type: STRING)," +
                        " $.b == 3 => $.a3.get(type: STRING)," +
                        " default => \"hello\")",
                expected);
    }
}
