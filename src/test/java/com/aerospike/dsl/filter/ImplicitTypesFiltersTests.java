package com.aerospike.dsl.filter;

import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilters;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ImplicitTypesFiltersTests {

    @Test
    void implicitDefaultIntComparison() {
        assertThatThrownBy(() -> parseFilters("$.intBin1 < $.intBin2"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BIN_PART");
    }

    @Test
    void floatComparison() {
        assertThatThrownBy(() -> parseFilters("$.floatBin1 >= 100.25"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: FLOAT_OPERAND");
    }

    @Test
    void booleanComparison() {
        assertThatThrownBy(() -> parseFilters("$.boolBin1 == true"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessage("Operand type not supported: BOOL_OPERAND");
    }
}
