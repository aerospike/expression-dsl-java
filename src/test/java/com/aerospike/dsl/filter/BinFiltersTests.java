package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.InputContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static com.aerospike.dsl.util.TestUtils.parseFilterAndCompare;
import static org.assertj.core.api.Assertions.assertThat;

class BinFiltersTests {

    String NAMESPACE = "test1";
    List<com.aerospike.dsl.Index> INDEXES = List.of(
            Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
            Index.builder().namespace(NAMESPACE).bin("stringBin1").indexType(IndexType.STRING).binValuesRatio(1).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void binGT() {
        parseFilterAndCompare(InputContext.of("$.intBin1 > 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 101, Long.MAX_VALUE));
        parseFilterAndCompare(InputContext.of("$.intBin1 > -100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", -99, Long.MAX_VALUE));

        // Comparing Strings is not supported by secondary index Filters
        assertThat(parseFilter(InputContext.of("$.stringBin1 > 'text'"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(InputContext.of("$.stringBin1 > \"text\""), INDEX_FILTER_INPUT)).isNull();

        // "$.intBin1 > 100" and "100 < $.intBin1" represent identical Filters
        parseFilterAndCompare(InputContext.of("100 < $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 101, Long.MAX_VALUE));

        // Comparing Strings is not supported by secondary index Filters
        assertThat(parseFilter(InputContext.of("'text' > $.stringBin1"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(InputContext.of("\"text\" > $.stringBin1"), INDEX_FILTER_INPUT)).isNull();
    }

    @Test
    void binGT_logical_combinations() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        parseFilterAndCompare(InputContext.of("$.intBin1 > 100 and $.intBin2 < 1000"), IndexContext.of(NAMESPACE, indexes),
                Filter.range("intBin2", Long.MIN_VALUE, 999));

        parseFilterAndCompare(InputContext.of("$.intBin1 > 100 and $.intBin2 < 1000"), null); // No indexes given
    }

    @Test
    void binGE() {
        parseFilterAndCompare(InputContext.of("$.intBin1 >= 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 100, Long.MAX_VALUE));

        // "$.intBin1 >= 100" and "100 <= $.intBin1" represent identical Filters
        parseFilterAndCompare(InputContext.of("100 <= $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 100, Long.MAX_VALUE));
    }

    @Test
    void binLT() {
        parseFilterAndCompare(InputContext.of("$.intBin1 < 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 99));

        parseFilterAndCompare(InputContext.of("100 > $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 99));
    }

    @Test
    void binLE() {
        parseFilterAndCompare(InputContext.of("$.intBin1 <= 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 100));

        parseFilterAndCompare(InputContext.of("100 >= $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 100));
    }

    @Test
    void binEQ() {
        parseFilterAndCompare(InputContext.of("$.intBin1 == 100"), INDEX_FILTER_INPUT,
                Filter.equal("intBin1", 100));
        parseFilterAndCompare(InputContext.of("100 == $.intBin1"), INDEX_FILTER_INPUT,
                Filter.equal("intBin1", 100));

        parseFilterAndCompare(InputContext.of("$.stringBin1 == 'text'"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "text"));
        parseFilterAndCompare(InputContext.of("$.stringBin1 == \"text\""), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "text"));
    }

    @Test
    void binNOTEQ() {
        // NOT EQUAL is not supported by secondary index filter
        assertThat(parseFilter(InputContext.of("$.intBin1 != 100"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(InputContext.of("$.stringBin1 != 'text'"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(InputContext.of("$.stringBin1 != \"text\""), INDEX_FILTER_INPUT)).isNull();

        assertThat(parseFilter(InputContext.of("100 != $.intBin1"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(InputContext.of("100 != 'text'"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(InputContext.of("100 != \"text\""), INDEX_FILTER_INPUT)).isNull();
    }
}
