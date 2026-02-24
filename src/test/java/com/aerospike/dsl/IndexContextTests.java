package com.aerospike.dsl;

import com.aerospike.dsl.client.query.IndexType;
import org.junit.jupiter.api.Test;

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
}
