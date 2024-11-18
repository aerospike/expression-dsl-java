package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilters;
import static com.aerospike.dsl.util.TestUtils.parseFiltersAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinFiltersTests {

    @Test
    void binGT() {
        parseFiltersAndCompare("$.intBin1 > 100",
                List.of(Filter.range("intBin1", 101, Long.MAX_VALUE)));
        parseFiltersAndCompare("$.intBin1 > -100",
                List.of(Filter.range("intBin1", -99, Long.MAX_VALUE)));

        assertThatThrownBy(() -> parseFilters("$.stringBin1 > 'text'"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Operand type not supported");
        assertThatThrownBy(() -> parseFilters("$.stringBin1 > \"text\""))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Operand type not supported");

        // "$.intBin1 > 100" and "100 < $.intBin1" represent identical Filters
        parseFiltersAndCompare("100 < $.intBin1",
                List.of(Filter.range("intBin1", 101, Long.MAX_VALUE)));

        assertThatThrownBy(() -> parseFilters("'text' > $.stringBin1"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Operand type not supported");
        assertThatThrownBy(() -> parseFilters("\"text\" > $.stringBin1"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Operand type not supported");
    }

    @Test
    void binGE() {
        parseFiltersAndCompare("$.intBin1 >= 100",
                List.of(Filter.range("intBin1", 100, Long.MAX_VALUE)));

        // "$.intBin1 >= 100" and "100 <= $.intBin1" represent identical Filters
        parseFiltersAndCompare("100 <= $.intBin1",
                List.of(Filter.range("intBin1", 100, Long.MAX_VALUE)));
    }

    @Test
    void binLT() {
        parseFiltersAndCompare("$.intBin1 < 100",
                List.of(Filter.range("intBin1", Long.MIN_VALUE, 99)));

        parseFiltersAndCompare("100 > $.intBin1",
                List.of(Filter.range("intBin1", Long.MIN_VALUE, 99)));
    }

    @Test
    void binLE() {
        parseFiltersAndCompare("$.intBin1 <= 100",
                List.of(Filter.range("intBin1", Long.MIN_VALUE, 100)));

        parseFiltersAndCompare("100 >= $.intBin1",
                List.of(Filter.range("intBin1", Long.MIN_VALUE, 100)));
    }

    @Test
    void binEQ() {
        parseFiltersAndCompare("$.intBin1 == 100",
                List.of(Filter.equal("intBin1", 100)));
        parseFiltersAndCompare("100 == $.intBin1",
                List.of(Filter.equal("intBin1", 100)));

        parseFiltersAndCompare("$.stringBin1 == 'text'",
                List.of(Filter.equal("stringBin1", "text")));
        parseFiltersAndCompare("$.stringBin1 == \"text\"",
                List.of(Filter.equal("stringBin1", "text")));
    }

    @Test
    void binNOTEQ() {
        // NOT EQUAL is not supported by secondary index filter
        assertThatThrownBy(() -> parseFilters("$.intBin1 != 100"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("$.stringBin1 != 'text'"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("$.stringBin1 != \"text\""))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("100 != $.intBin1"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("100 != 'text'"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("100 != \"text\""))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
    }
}
