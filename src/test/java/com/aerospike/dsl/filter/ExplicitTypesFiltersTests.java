package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilters;
import static com.aerospike.dsl.util.TestUtils.parseFiltersAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExplicitTypesFiltersTests {

    @Test
    void integerComparison() {
        parseFiltersAndCompare("$.intBin1.get(type: INT) > 5",
                List.of(Filter.range("intBin1", 6, Long.MAX_VALUE)));

        parseFiltersAndCompare("5 < $.intBin1.get(type: INT)",
                List.of(Filter.range("intBin1", 6, Long.MAX_VALUE)));
    }

    @Test
    void stringComparison() {
        parseFiltersAndCompare("$.stringBin1.get(type: STRING) == \"yes\"",
                List.of(Filter.equal("stringBin1", "yes")));

        parseFiltersAndCompare("$.stringBin1.get(type: STRING) == 'yes'",
                List.of(Filter.equal("stringBin1", "yes")));

        parseFiltersAndCompare("\"yes\" == $.stringBin1.get(type: STRING)",
                List.of(Filter.equal("stringBin1", "yes")));

        parseFiltersAndCompare("'yes' == $.stringBin1.get(type: STRING)",
                List.of(Filter.equal("stringBin1", "yes")));
    }

    @Test
    void stringComparisonNegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() -> parseFilters("$.stringBin1.get(type: STRING) == yes"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse right operand");
    }

    @Test
    void blobComparison() {
        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        parseFiltersAndCompare("$.blobBin1.get(type: BLOB) == \"" + encodedString + "\"",
                List.of(Filter.equal("blobBin1", data)));

        // Reverse
        parseFiltersAndCompare("\"" + encodedString + "\" == $.blobBin1.get(type: BLOB)",
                List.of(Filter.equal("blobBin1", data)));
    }

    @Test
    void floatComparison() {
        // No float support in secondary index filter
        assertThatThrownBy(() -> parseFilters("$.floatBin1.get(type: FLOAT) == 1.5"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: FLOAT_OPERAND");

        assertThatThrownBy(() -> parseFilters("1.5 == $.floatBin1.get(type: FLOAT)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: FLOAT_OPERAND");
    }

    @Test
    void booleanComparison() {
        // No boolean support in secondary index filter
        assertThatThrownBy(() -> parseFilters("$.boolBin1.get(type: BOOL) == true"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BOOL_OPERAND");

        assertThatThrownBy(() -> parseFilters("true == $.boolBin1.get(type: BOOL)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BOOL_OPERAND");
    }

    @Test
    void negativeBooleanComparison() {
        assertThatThrownBy(() -> parseFilters("$.boolBin1.get(type: BOOL) == 5"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Cannot compare BOOL to INT");
    }

    @Test
    void listComparison_constantOnRightSide() {
        assertThatThrownBy(() -> parseFilters("$.listBin1.get(type: LIST) == [100]"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: LIST_OPERAND");
    }

    @Test
    void listComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilters("$.listBin1.get(type: LIST) == [yes, of course]"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse list operand");
    }

    @Test
    void listComparison_constantOnLeftSide() {
        assertThatThrownBy(() -> parseFilters("[100] == $.listBin1.get(type: LIST)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: LIST_OPERAND");
    }

    @Test
    void listComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilters("[yes, of course] == $.listBin1.get(type: LIST)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");
    }

    @Test
    void mapComparison_constantOnRightSide() {
        assertThatThrownBy(() -> parseFilters("$.mapBin1.get(type: MAP) == {100:100}"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: MAP_OPERAND");
    }

    @Test
    void mapComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilters("$.mapBin1.get(type: MAP) == {yes, of course}"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse map operand");
    }

    @Test
    void mapComparison_constantOnLeftSide() {
        assertThatThrownBy(() -> parseFilters("{100:100} == $.mapBin1.get(type: MAP)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: MAP_OPERAND");
    }

    @Test
    void mapComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilters("{yes, of course} == $.mapBin1.get(type: MAP)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");
    }

    @Test
    void twoStringBinsComparison() {
        assertThatThrownBy(() -> parseFilters("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BIN_PART");
    }

    @Test
    void twoIntegerBinsComparison() {
        assertThatThrownBy(() -> parseFilters("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BIN_PART");
    }

    @Test
    void twoFloatBinsComparison() {
        assertThatThrownBy(() -> parseFilters("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BIN_PART");
    }

    @Test
    void twoBlobBinsComparison() {
        assertThatThrownBy(() -> parseFilters("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BIN_PART");
    }

    @Test
    void negativeTwoDifferentBinTypesComparison() {
        assertThatThrownBy(() -> parseFilters("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BIN_PART");
    }
}
