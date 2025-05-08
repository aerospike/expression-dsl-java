package com.aerospike.dsl.filter;

import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static org.assertj.core.api.Assertions.assertThat;

public class ImplicitTypesFiltersTests {

    @Test
    void implicitDefaultIntComparison() {
        assertThat(parseFilter("$.intBin1 < $.intBin2")).isNull();
    }

    @Test
    void floatComparison() {
        assertThat(parseFilter("$.floatBin1 >= 100.25")).isNull();
    }

    @Test
    void booleanComparison() {
        assertThat(parseFilter("$.boolBin1 == true")).isNull();
    }
}
