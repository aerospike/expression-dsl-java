package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.index.Index;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static com.aerospike.dsl.util.TestUtils.parseFilterAndCompare;
import static org.assertj.core.api.Assertions.assertThat;

class BinFiltersTests {

    @Test
    void binGT() {
        parseFilterAndCompare("$.intBin1 > 100",
                Filter.range("intBin1", 101, Long.MAX_VALUE));
        parseFilterAndCompare("$.intBin1 > -100",
                Filter.range("intBin1", -99, Long.MAX_VALUE));

        // Comparing Strings is not supported by secondary index Filters
        assertThat(parseFilter("$.stringBin1 > 'text'")).isNull();
        assertThat(parseFilter("$.stringBin1 > \"text\"")).isNull();

        // "$.intBin1 > 100" and "100 < $.intBin1" represent identical Filters
        parseFilterAndCompare("100 < $.intBin1",
                Filter.range("intBin1", 101, Long.MAX_VALUE));

        // Comparing Strings is not supported by secondary index Filters
        assertThat(parseFilter("'text' > $.stringBin1")).isNull();
        assertThat(parseFilter("\"text\" > $.stringBin1")).isNull();
    }

    @Test
    void binGT_logical_combinations() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        parseFilterAndCompare("$.intBin1 > 100 and $.intBin2 < 1000", "test1", indexes,
                Filter.range("intBin2", Long.MIN_VALUE, 999));

        parseFilterAndCompare("$.intBin1 > 100 and $.intBin2 < 1000",
                null);
    }

    @Test
    void binGE() {
        parseFilterAndCompare("$.intBin1 >= 100",
                Filter.range("intBin1", 100, Long.MAX_VALUE));

        // "$.intBin1 >= 100" and "100 <= $.intBin1" represent identical Filters
        parseFilterAndCompare("100 <= $.intBin1",
                Filter.range("intBin1", 100, Long.MAX_VALUE));
    }

    @Test
    void binLT() {
        parseFilterAndCompare("$.intBin1 < 100",
                Filter.range("intBin1", Long.MIN_VALUE, 99));

        parseFilterAndCompare("100 > $.intBin1",
                Filter.range("intBin1", Long.MIN_VALUE, 99));
    }

    @Test
    void binLE() {
        parseFilterAndCompare("$.intBin1 <= 100",
                Filter.range("intBin1", Long.MIN_VALUE, 100));

        parseFilterAndCompare("100 >= $.intBin1",
                Filter.range("intBin1", Long.MIN_VALUE, 100));
    }

    @Test
    void binEQ() {
        parseFilterAndCompare("$.intBin1 == 100",
                Filter.equal("intBin1", 100));
        parseFilterAndCompare("100 == $.intBin1",
                Filter.equal("intBin1", 100));

        parseFilterAndCompare("$.stringBin1 == 'text'",
                Filter.equal("stringBin1", "text"));
        parseFilterAndCompare("$.stringBin1 == \"text\"",
                Filter.equal("stringBin1", "text"));
    }

    @Test
    void binNOTEQ() {
        // NOT EQUAL is not supported by secondary index filter
        assertThat(parseFilter("$.intBin1 != 100")).isNull();
        assertThat(parseFilter("$.stringBin1 != 'text'")).isNull();
        assertThat(parseFilter("$.stringBin1 != \"text\"")).isNull();

        assertThat(parseFilter("100 != $.intBin1")).isNull();
        assertThat(parseFilter("100 != 'text'")).isNull();
        assertThat(parseFilter("100 != \"text\"")).isNull();
    }
}
