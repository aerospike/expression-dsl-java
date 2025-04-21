package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExplicitTypesFiltersTests {

    @Test
    void integerComparison() {
        TestUtils.parseFilterAndCompare("$.intBin1.get(type: INT) > 5",
                Filter.range("intBin1", 6, Long.MAX_VALUE));

        TestUtils.parseFilterAndCompare("5 < $.intBin1.get(type: INT)",
                Filter.range("intBin1", 6, Long.MAX_VALUE));
    }

    @Test
    void stringComparison() {
        TestUtils.parseFilterAndCompare("$.stringBin1.get(type: STRING) == \"yes\"",
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare("$.stringBin1.get(type: STRING) == 'yes'",
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare("\"yes\" == $.stringBin1.get(type: STRING)",
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare("'yes' == $.stringBin1.get(type: STRING)",
                Filter.equal("stringBin1", "yes"));
    }

    @Test
    void stringComparisonNegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() -> parseFilter("$.stringBin1.get(type: STRING) == yes"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse right operand");
    }

    @Test
    void blobComparison() {
        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        TestUtils.parseFilterAndCompare("$.blobBin1.get(type: BLOB) == \"" + encodedString + "\"",
                Filter.equal("blobBin1", data));

        // Reverse
        TestUtils.parseFilterAndCompare("\"" + encodedString + "\" == $.blobBin1.get(type: BLOB)",
                Filter.equal("blobBin1", data));
    }

    @Test
    void floatComparison() {
        // No float support in secondary index filter
        assertThat(parseFilter("$.floatBin1.get(type: FLOAT) == 1.5")).isNull();
        assertThat(parseFilter("1.5 == $.floatBin1.get(type: FLOAT)")).isNull();
    }

    @Test
    void booleanComparison() {
        // No boolean support in secondary index filter
        assertThat(parseFilter("$.boolBin1.get(type: BOOL) == true")).isNull();
        assertThat(parseFilter("true == $.boolBin1.get(type: BOOL)")).isNull();
    }

    @Test
    void negativeBooleanComparison() {
        assertThatThrownBy(() -> parseFilter("$.boolBin1.get(type: BOOL) == 5"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Cannot compare BOOL to INT");
    }

    @Test
    void listComparison_constantOnRightSide() {
        // Not supported by secondary index filter
        assertThat(parseFilter("$.listBin1.get(type: LIST) == [100]")).isNull();
    }

    @Test
    void listComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter("$.listBin1.get(type: LIST) == [yes, of course]"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse list operand");
    }

    @Test
    void listComparison_constantOnLeftSide() {
        assertThat(parseFilter("[100] == $.listBin1.get(type: LIST)")).isNull();
    }

    @Test
    void listComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter("[yes, of course] == $.listBin1.get(type: LIST)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");
    }

    @Test
    void mapComparison_constantOnRightSide() {
        assertThat(parseFilter("$.mapBin1.get(type: MAP) == {100:100}")).isNull();
    }

    @Test
    void mapComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter("$.mapBin1.get(type: MAP) == {yes, of course}"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Unable to parse map operand");
    }

    @Test
    void mapComparison_constantOnLeftSide() {
        assertThat(parseFilter("{100:100} == $.mapBin1.get(type: MAP)")).isNull();
    }

    @Test
    void mapComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter("{yes, of course} == $.mapBin1.get(type: MAP)"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Could not parse given input, wrong syntax");
    }

    @Test
    void twoStringBinsComparison() {
        assertThat(parseFilter("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)")).isNull();
    }

    @Test
    void twoIntegerBinsComparison() {
        assertThat(parseFilter("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)")).isNull();
    }

    @Test
    void twoFloatBinsComparison() {
        assertThat(parseFilter("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)")).isNull();
    }

    @Test
    void twoBlobBinsComparison() {
        assertThat(parseFilter("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)")).isNull();
    }

    @Test
    void differentBinTypes_nullResult() {
        assertThat(parseFilter("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)")).isNull();
    }
}
