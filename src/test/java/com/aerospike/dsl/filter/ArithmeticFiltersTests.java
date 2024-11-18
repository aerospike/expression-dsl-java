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

        assertThatThrownBy(() -> parseFilters("($.apples + $.bananas + 5) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Could not parse given input, wrong syntax");
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

        parseFiltersAndCompare("($.apples - 5) > -10",
                List.of(Filter.range("apples", -10 + 5 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples - 5) >= -10",
                List.of(Filter.range("apples", -10 + 5, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples - 5) < -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -10 + 5 - 1)));
        parseFiltersAndCompare("($.apples - 5) <= -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -10 + 5)));

        parseFiltersAndCompare("(9 - $.bananas) > 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 9 - 10 - 1)));
        parseFiltersAndCompare("(9 - $.bananas) >= 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 9 - 10)));
        parseFiltersAndCompare("(9 - $.bananas) < 10",
                List.of(Filter.range("bananas", 9 - 10 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("(9 - $.bananas) <= 10",
                List.of(Filter.range("bananas", 9 - 10, Long.MAX_VALUE)));

        assertThatThrownBy(() -> parseFilters("($.apples - $.bananas) > 10"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("($.apples - $.bananas - 5) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Could not parse given input, wrong syntax");
    }

    @Test
    void mul() {
        assertThatThrownBy(() -> parseFilters("($.apples * $.bananas) > 10"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        parseFiltersAndCompare("($.apples * 5) > 10",
                List.of(Filter.range("apples", 10 / 5 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples * 5) >= 10",
                List.of(Filter.range("apples", 10 / 5, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples * 5) < 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 / 5 - 1)));
        parseFiltersAndCompare("($.apples * 5) <= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 / 5)));

        parseFiltersAndCompare("(9 * $.bananas) > 10",
                List.of(Filter.range("bananas", 10 / 9 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("(9 * $.bananas) >= 10",
                List.of(Filter.range("bananas", 10 / 9, Long.MAX_VALUE)));
        parseFiltersAndCompare("(9 * $.bananas) < 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 10 / 9)));
        parseFiltersAndCompare("(9 * $.bananas) <= 10",
                List.of(Filter.range("bananas", Long.MIN_VALUE, 10 / 9)));

        parseFiltersAndCompare("($.apples * -5) > 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 / -5 - 1)));
        parseFiltersAndCompare("($.apples * -5) >= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 10 / -5)));
        parseFiltersAndCompare("($.apples * -5) < 10",
                List.of(Filter.range("apples", 10 / -5 + 1, Long.MAX_VALUE)));
        parseFiltersAndCompare("($.apples * -5) <= 10",
                List.of(Filter.range("apples", 10 / -5, Long.MAX_VALUE)));

        assertThatThrownBy(() -> parseFilters("(0 * $.bananas) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Cannot divide by zero");

        parseFiltersAndCompare("(9 * $.bananas) > 0",
                List.of(Filter.range("bananas", 0 / 9 + 1, Long.MAX_VALUE)));

        assertThatThrownBy(() -> parseFilters("($.apples * $.bananas - 5) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Could not parse given input, wrong syntax");
    }

    @Test
    void div_twoBins() {
        assertThatThrownBy(() -> parseFilters("($.apples / $.bananas) <= 10"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
    }

    @Test
    void div_binIsDivided_leftNumberIsLarger() {
        parseFiltersAndCompare("($.apples / 50) > 10",
                List.of(Filter.range("apples", 50 * 10 + 1, Long.MAX_VALUE))); // [501, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 50) >= 10",
                List.of(Filter.range("apples", 50 * 10, Long.MAX_VALUE))); // [500, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 50) < 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 50 * 10 - 1))); // [-2^63, 499]
        parseFiltersAndCompare("($.apples / 50) <= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 50 * 10))); // [-2^63, 500]

        parseFiltersAndCompare("($.apples / -50) > 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -50 * 10 - 1))); // [-2^63, -501]
        parseFiltersAndCompare("($.apples / -50) >= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -50 * 10))); // [-2^63, -500]
        parseFiltersAndCompare("($.apples / -50) < 10",
                List.of(Filter.range("apples", -50 * 10 + 1, Long.MAX_VALUE))); // [-499, 2^63 - 1]
        parseFiltersAndCompare("($.apples / -50) <= 10",
                List.of(Filter.range("apples", -50 * 10, Long.MAX_VALUE))); // [-500, 2^63 - 1]

        parseFiltersAndCompare("($.apples / 50) > -10",
                List.of(Filter.range("apples", 50 * -10 + 1, Long.MAX_VALUE))); // [-499, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 50) >= -10",
                List.of(Filter.range("apples", 50 * -10, Long.MAX_VALUE))); // [-500, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 50) < -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 50 * -10 - 1))); // [-2^63, -501]
        parseFiltersAndCompare("($.apples / 50) <= -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 50 * -10))); // [-2^63, -500]

        parseFiltersAndCompare("($.apples / -50) > -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -50 * -10 - 1))); // [-2^63, 499]
        parseFiltersAndCompare("($.apples / -50) >= -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -10 * -50))); // [-2^63, 500]
        parseFiltersAndCompare("($.apples / -50) < -10",
                List.of(Filter.range("apples", -10 * -50 + 1, Long.MAX_VALUE))); // [501, 2^63 - 1]
        parseFiltersAndCompare("($.apples / -50) <= -10",
                List.of(Filter.range("apples", -10 * -50, Long.MAX_VALUE))); // [500, 2^63 - 1]
    }

    @Test
    void div_binIsDivided_leftNumberIsSmaller() {
        parseFiltersAndCompare("($.apples / 5) > 10",
                List.of(Filter.range("apples", 5 * 10 + 1, Long.MAX_VALUE))); // [51, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 5) >= 10",
                List.of(Filter.range("apples", 5 * 10, Long.MAX_VALUE))); // [50, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 5) < 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 5 * 10 - 1))); // [-2^63, 49]
        parseFiltersAndCompare("($.apples / 5) <= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 5 * 10))); // [-2^63, 50]

        parseFiltersAndCompare("($.apples / -5) > 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -5 * 10 - 1))); // [-2^63, -51]
        parseFiltersAndCompare("($.apples / -5) >= 10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -5 * 10))); // [-2^63, -50]
        parseFiltersAndCompare("($.apples / -5) < 10",
                List.of(Filter.range("apples", -5 * 10 + 1, Long.MAX_VALUE))); // [-49, 2^63 - 1]
        parseFiltersAndCompare("($.apples / -5) <= 10",
                List.of(Filter.range("apples", -5 * 10, Long.MAX_VALUE))); // [-50, 2^63 - 1]

        parseFiltersAndCompare("($.apples / 5) > -10",
                List.of(Filter.range("apples", 5 * -10 + 1, Long.MAX_VALUE))); // [-49, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 5) >= -10",
                List.of(Filter.range("apples", 5 * -10, Long.MAX_VALUE))); // [-50, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 5) < -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 5 * -10 - 1))); // [-2^63, -51]
        parseFiltersAndCompare("($.apples / 5) <= -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, 5 * -10))); // [-2^63, -50]

        parseFiltersAndCompare("($.apples / -5) > -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -5 * -10 - 1))); // [-2^63, 49]
        parseFiltersAndCompare("($.apples / -5) >= -10",
                List.of(Filter.range("apples", Long.MIN_VALUE, -10 * -5))); // [-2^63, 50]
        parseFiltersAndCompare("($.apples / -5) < -10",
                List.of(Filter.range("apples", -10 * -5 + 1, Long.MAX_VALUE))); // [51, 2^63 - 1]
        parseFiltersAndCompare("($.apples / -5) <= -10",
                List.of(Filter.range("apples", -10 * -5, Long.MAX_VALUE))); // [50, 2^63 - 1]
    }

    @Test
    void div_binIsDivided_leftNumberEqualsRight() {
        parseFiltersAndCompare("($.apples / 5) > 5",
                List.of(Filter.range("apples", 5 * 5 + 1, Long.MAX_VALUE))); // [26, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 5) >= 5",
                List.of(Filter.range("apples", 5 * 5, Long.MAX_VALUE))); // [25, 2^63 - 1]
        parseFiltersAndCompare("($.apples / 5) < 5",
                List.of(Filter.range("apples", Long.MIN_VALUE, 5 * 5 - 1))); // [-2^63, 24]
        parseFiltersAndCompare("($.apples / 5) <= 5",
                List.of(Filter.range("apples", Long.MIN_VALUE, 5 * 5))); // [-2^63, 25]

        parseFiltersAndCompare("($.apples / -5) > -5",
                List.of(Filter.range("apples", Long.MIN_VALUE, -5 * -5 - 1))); // [-2^63, 24]
        parseFiltersAndCompare("($.apples / -5) >= -5",
                List.of(Filter.range("apples", Long.MIN_VALUE, -5 * -5))); // [-2^63, 25]
        parseFiltersAndCompare("($.apples / -5) < -5",
                List.of(Filter.range("apples", -5 * -5 + 1, Long.MAX_VALUE))); // [26, 2^63 - 1]
        parseFiltersAndCompare("($.apples / -5) <= -5",
                List.of(Filter.range("apples", -5 * -5, Long.MAX_VALUE))); // [25, 2^63 - 1]
    }

    @Test
    void div_binIsDivisor_leftNumberIsLarger() {
        parseFiltersAndCompare("(90 / $.bananas) > 10",
                List.of(Filter.range("bananas", 1, 90 / 10 - 1))); // [1,8]
        parseFiltersAndCompare("(90 / $.bananas) >= 10",
                List.of(Filter.range("bananas", 1, 90 / 10))); // [1,9]
        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) < 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) <= 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) > -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) >= -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        parseFiltersAndCompare("(90 / $.bananas) < -10",
                List.of(Filter.range("bananas", 90 / -10 + 1, -1))); // [-8, -1]
        parseFiltersAndCompare("(90 / $.bananas) <= -10",
                List.of(Filter.range("bananas", 90 / -10, -1))); // [-8, -1]

        parseFiltersAndCompare("(-90 / $.bananas) > 10",
                List.of(Filter.range("bananas", -90 / 10 + 1, -1))); // [-8, -1]
        parseFiltersAndCompare("(90 / $.bananas) >= 10",
                List.of(Filter.range("bananas", 1, 90 / 10))); // [1,9]
        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) < 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) <= 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) > -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) >= -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        parseFiltersAndCompare("(-90 / $.bananas) < -10",
                List.of(Filter.range("bananas", 1L, -90 / -10 - 1))); // [1, 8]
        parseFiltersAndCompare("(-90 / $.bananas) <= -10",
                List.of(Filter.range("bananas", 1L, -90 / -10))); // [1, 9]
    }

    @Test
    void div_binIsDivisor_leftNumberIsSmaller() {
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) > 10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) >= 10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) < 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) <= 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) > -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) >= -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) < -10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) <= -10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) > 10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) >= 10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) < 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) <= 10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) > -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) >= -10")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) < -10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-9 / $.bananas) <= -10")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(0 / $.bananas) > 10"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(9 / $.bananas) > 0"))
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("Cannot divide by zero");
    }

    @Test
    void div_binIsDivisor_leftNumberEqualsRight() {
        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) > 90")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        parseFiltersAndCompare("(90 / $.bananas) >= 90",
                List.of(Filter.range("bananas", 90 / 90, 90 / 90))); // [1, 1]
        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) < 90")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(90 / $.bananas) <= 90")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");

        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) > -90")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) >= -90")) // maximal range is all numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        assertThatThrownBy(() -> parseFilters("(-90 / $.bananas) < -90")) // no integer numbers
                .isInstanceOf(AerospikeDSLException.class)
                .hasMessageContaining("The operation is not supported by secondary index filter");
        parseFiltersAndCompare("(-90 / $.bananas) <= -90",
                List.of(Filter.range("bananas", 1L, 90 / 90))); // [1, 1]
    }
}
