package com.aerospike.dsl.filter;

import com.aerospike.dsl.InputContext;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static org.assertj.core.api.Assertions.assertThat;

public class ImplicitTypesFiltersTests {

    @Test
    void implicitDefaultIntComparison() {
        assertThat(parseFilter(InputContext.of("$.intBin1 < $.intBin2"))).isNull();
    }

    @Test
    void floatComparison() {
        assertThat(parseFilter(InputContext.of("$.floatBin1 >= 100.25"))).isNull();
    }

    @Test
    void booleanComparison() {
        assertThat(parseFilter(InputContext.of("$.boolBin1 == true"))).isNull();
    }
}
