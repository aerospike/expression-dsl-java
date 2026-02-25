package com.aerospike.dsl;

import com.aerospike.dsl.client.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexContextTests {

    private static final String NAMESPACE = "test";
    private static final Index VALID_INDEX = Index.builder()
            .namespace(NAMESPACE)
            .bin("bin1")
            .indexType(IndexType.NUMERIC)
            .binValuesRatio(0)
            .build();

    @Test
    void of_rejects_null_namespace() {
        assertThatThrownBy(() -> IndexContext.of(null, List.of(VALID_INDEX)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void of_rejects_blank_namespace() {
        assertThatThrownBy(() -> IndexContext.of("  ", List.of(VALID_INDEX)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void of_accepts_valid_namespace() {
        IndexContext ctx = IndexContext.of(NAMESPACE, Collections.emptyList());

        assertThat(ctx.getNamespace()).isEqualTo(NAMESPACE);
        assertThat(ctx.getIndexes()).isEmpty();
    }

    @Test
    void of_3arg_rejects_null_namespace() {
        assertThatThrownBy(() -> IndexContext.of(null, List.of(VALID_INDEX), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void of_3arg_rejects_blank_namespace() {
        assertThatThrownBy(() -> IndexContext.of("", List.of(VALID_INDEX), "idx1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void withBinHint_rejects_null_namespace() {
        assertThatThrownBy(() -> IndexContext.withBinHint(null, List.of(VALID_INDEX), "bin1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void withBinHint_rejects_blank_namespace() {
        assertThatThrownBy(() -> IndexContext.withBinHint("", List.of(VALID_INDEX), "bin1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void withBinHint_null_bin_returns_all_indexes() {
        Collection<Index> indexes = List.of(VALID_INDEX);
        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, null);

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_empty_bin_returns_all_indexes() {
        Collection<Index> indexes = List.of(VALID_INDEX);
        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_single_match_returns_that_index() {
        Index other = Index.builder().namespace(NAMESPACE).bin("bin2")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(VALID_INDEX, other);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "bin1");

        assertThat(ctx.getIndexes()).containsExactly(VALID_INDEX);
    }

    @Test
    void withBinHint_multiple_matches_returns_all_indexes() {
        Index second = Index.builder().namespace(NAMESPACE).bin("bin1")
                .indexType(IndexType.STRING).binValuesRatio(5).build();
        Collection<Index> indexes = List.of(VALID_INDEX, second);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_no_match_returns_all_indexes() {
        Collection<Index> indexes = List.of(VALID_INDEX);
        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "nonExistentBin");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_namespace_mismatch_returns_all_indexes() {
        Index wrongNs = Index.builder().namespace("other_ns").bin("bin1")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(wrongNs);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }
}
