package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilters;
import static com.aerospike.dsl.util.TestUtils.parseFiltersAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArithmeticFiltersTests {

    @Test
    void add() {
        assertThatThrownBy(() -> parseFilters("($.apples + $.bananas) > 10"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        parseFiltersAndCompare("($.apples + 5) > 10",
                List.of(Filter.range("apples", 10 - 5 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples + 5) >= 10",
                List.of(Filter.range("apples", 10 - 5, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples + 5) < 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 - 5 - 1)));
        parseFiltersAndCompare("($.apples + 5) <= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 - 5)));

        parseFiltersAndCompare("(9 + $.bananas) > 10",
                List.of(Filter.range("bananas", 10 - 9 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("(9 + $.bananas) >= 10",
                List.of(Filter.range("bananas", 10 - 9, Long.MAX_VALUE)));
        parseFiltersAndCompare("(9 + $.bananas) < 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 10 - 9 - 1)));
        parseFiltersAndCompare("(9 + $.bananas) <= 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 10 - 9)));

        assertThatThrownBy(() -> parseFilters("(5.2 + $.bananas) > 10.2"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
    }

    @Test
    void sub() {
        assertThatThrownBy(() -> parseFilters("($.apples - $.bananas) > 10"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        parseFiltersAndCompare("($.apples - 5) > 10",
                List.of(Filter.range("apples", 10 + 5 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples - 5) >= 10",
                List.of(Filter.range("apples", 10 + 5, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples - 5) < 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 + 5 - 1)));
        parseFiltersAndCompare("($.apples - 5) <= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 + 5)));

        parseFiltersAndCompare("(9 - $.bananas) > 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 9 - 10 - 1)));
        parseFiltersAndCompare("(9 - $.bananas) >= 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 9 - 10)));
        parseFiltersAndCompare("(9 - $.bananas) < 10",
                List.of(Filter.range("bananas", 9 - 10 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("(9 - $.bananas) <= 10",
                List.of(Filter.range("bananas", 9 - 10, Long.MAX_VALUE)));
    }
}
