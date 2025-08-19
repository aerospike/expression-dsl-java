package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.InputContext;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExplicitTypesFiltersTests {

    String NAMESPACE = "test1";
    List<Index> INDEXES = List.of(
            Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
            Index.builder().namespace("test1").bin("stringBin1").indexType(IndexType.STRING).binValuesRatio(1).build(),
            Index.builder().namespace("test1").bin("blobBin1").indexType(IndexType.BLOB).binValuesRatio(1).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void integerComparison() {
        // Namespace and indexes must be given to create a Filter
        TestUtils.parseFilterAndCompare(InputContext.of("$.intBin1.get(type: INT) > 5"), null);

        TestUtils.parseFilterAndCompare(InputContext.of("$.intBin1.get(type: INT) > 5"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 6, Long.MAX_VALUE));

        TestUtils.parseFilterAndCompare(InputContext.of("5 < $.intBin1.get(type: INT)"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 6, Long.MAX_VALUE));
    }

    @Test
    void stringComparison() {
        TestUtils.parseFilterAndCompare(InputContext.of("$.stringBin1.get(type: STRING) == \"yes\""), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare(InputContext.of("$.stringBin1.get(type: STRING) == 'yes'"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare(InputContext.of("\"yes\" == $.stringBin1.get(type: STRING)"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare(InputContext.of("'yes' == $.stringBin1.get(type: STRING)"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));
    }

    @Test
    void stringComparisonNegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() -> parseFilter(InputContext.of("$.stringBin1.get(type: STRING) == yes")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Unable to parse right operand");
    }

    @Test
    void blobComparison() {
        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        TestUtils.parseFilterAndCompare(InputContext.of("$.blobBin1.get(type: BLOB) == \"" + encodedString + "\""),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));

        // Reverse
        TestUtils.parseFilterAndCompare(InputContext.of("\"" + encodedString + "\" == $.blobBin1.get(type: BLOB)"),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));
    }

    @Test
    void floatComparison() {
        // No float support in secondary index filter
        assertThat(parseFilter(InputContext.of("$.floatBin1.get(type: FLOAT) == 1.5"))).isNull();
        assertThat(parseFilter(InputContext.of("1.5 == $.floatBin1.get(type: FLOAT)"))).isNull();
    }

    @Test
    void booleanComparison() {
        // No boolean support in secondary index filter
        assertThat(parseFilter(InputContext.of("$.boolBin1.get(type: BOOL) == true"))).isNull();
        assertThat(parseFilter(InputContext.of("true == $.boolBin1.get(type: BOOL)"))).isNull();
    }

    @Test
    void negativeBooleanComparison() {
        assertThatThrownBy(() -> parseFilter(InputContext.of("$.boolBin1.get(type: BOOL) == 5")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Cannot compare BOOL to INT");
    }

    @Test
    void listComparison_constantOnRightSide() {
        // Not supported by secondary index filter
        assertThat(parseFilter(InputContext.of("$.listBin1.get(type: LIST) == [100]"))).isNull();
    }

    @Test
    void listComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(InputContext.of("$.listBin1.get(type: LIST) == [yes, of course]")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Unable to parse list operand");
    }

    @Test
    void listComparison_constantOnLeftSide() {
        assertThat(parseFilter(InputContext.of("[100] == $.listBin1.get(type: LIST)"))).isNull();
    }

    @Test
    void listComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(InputContext.of("[yes, of course] == $.listBin1.get(type: LIST)")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Could not parse given DSL expression input");
    }

    @Test
    void mapComparison_constantOnRightSide() {
        assertThat(parseFilter(InputContext.of("$.mapBin1.get(type: MAP) == {100:100}"))).isNull();
    }

    @Test
    void mapComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(InputContext.of("$.mapBin1.get(type: MAP) == {yes, of course}")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Unable to parse map operand");
    }

    @Test
    void mapComparison_constantOnLeftSide() {
        assertThat(parseFilter(InputContext.of("{100:100} == $.mapBin1.get(type: MAP)"))).isNull();
    }

    @Test
    void mapComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(InputContext.of("{yes, of course} == $.mapBin1.get(type: MAP)")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Could not parse given DSL expression input");
    }

    @Test
    void twoStringBinsComparison() {
        assertThat(parseFilter(InputContext.of("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)")))
                .isNull();
    }

    @Test
    void twoIntegerBinsComparison() {
        assertThat(parseFilter(InputContext.of("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)"))).isNull();
    }

    @Test
    void twoFloatBinsComparison() {
        assertThat(parseFilter(InputContext.of("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)"))).isNull();
    }

    @Test
    void twoBlobBinsComparison() {
        assertThat(parseFilter(InputContext.of("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)"))).isNull();
    }

    @Test
    void negativeTwoDifferentBinTypesComparison() {
        assertThatThrownBy(() -> parseFilter(InputContext.of("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)")))
                .isInstanceOf(DslParseException.class)
                .hasMessage("Cannot compare STRING to FLOAT");
    }
}
