package com.aerospike.dsl.filter;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexFilterInput;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilter;
import static com.aerospike.dsl.util.TestUtils.parseFilterAndCompare;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArithmeticFiltersTests {

    String NAMESPACE = "test1";
    Collection<Index> INDEXES = List.of(
            Index.builder().namespace("test1").bin("apples").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
            Index.builder().namespace("test1").bin("bananas").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
    );
    IndexFilterInput INDEX_FILTER_INPUT = IndexFilterInput.of(NAMESPACE, INDEXES);

    @Test
    void add() {
        // not supported by secondary index filter
        assertThat(parseFilter("($.apples + $.bananas) > 10", INDEX_FILTER_INPUT)).isNull();

        parseFilterAndCompare("($.apples + 5) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 - 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples + 5) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 - 5, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples + 5) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 - 5 - 1));
        parseFilterAndCompare("($.apples + 5) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 - 5));

        parseFilterAndCompare("(9 + $.bananas) > 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 - 9 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("(9 + $.bananas) >= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 - 9, Long.MAX_VALUE));
        parseFilterAndCompare("(9 + $.bananas) < 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 - 9 - 1));
        parseFilterAndCompare("(9 + $.bananas) <= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 - 9));

        assertThat(parseFilter("(5.2 + $.bananas) > 10.2")).isNull(); // not supported by secondary index filter
        assertThatThrownBy(() -> parseFilter("($.apples + $.bananas + 5) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Could not parse given input, wrong syntax");
    }

    @Test
    void sub() {
        assertThat(parseFilter("($.apples - $.bananas) > 10", INDEX_FILTER_INPUT)).isNull(); // not supported by secondary index filter

        parseFilterAndCompare("($.apples - 5) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 + 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples - 5) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 + 5, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples - 5) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 + 5 - 1));
        parseFilterAndCompare("($.apples - 5) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 + 5));

        parseFilterAndCompare("($.apples - 5) > -10", INDEX_FILTER_INPUT,
                Filter.range("apples", -10 + 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples - 5) >= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", -10 + 5, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples - 5) < -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 + 5 - 1));
        parseFilterAndCompare("($.apples - 5) <= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 + 5));

        parseFilterAndCompare("(9 - $.bananas) > 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 9 - 10 - 1));
        parseFilterAndCompare("(9 - $.bananas) >= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 9 - 10));
        parseFilterAndCompare("(9 - $.bananas) < 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 9 - 10 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("(9 - $.bananas) <= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 9 - 10, Long.MAX_VALUE));

        assertThat(parseFilter("($.apples - $.bananas) > 10")).isNull(); // not supported by secondary index filter
        assertThatThrownBy(() -> parseFilter("($.apples - $.bananas - 5) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Could not parse given input, wrong syntax");
    }

    @Test
    void mul() {
        assertThat(parseFilter("($.apples * $.bananas) > 10", INDEX_FILTER_INPUT)).isNull(); // not supported by secondary index filter

        parseFilterAndCompare("($.apples * 5) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples * 5) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / 5, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples * 5) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / 5 - 1));
        parseFilterAndCompare("($.apples * 5) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / 5));

        parseFilterAndCompare("(9 * $.bananas) > 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 / 9 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("(9 * $.bananas) >= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 / 9, Long.MAX_VALUE));
        parseFilterAndCompare("(9 * $.bananas) < 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 / 9));
        parseFilterAndCompare("(9 * $.bananas) <= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 / 9));

        parseFilterAndCompare("($.apples * -5) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / -5 - 1));
        parseFilterAndCompare("($.apples * -5) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / -5));
        parseFilterAndCompare("($.apples * -5) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / -5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare("($.apples * -5) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / -5, Long.MAX_VALUE));

        assertThat(parseFilter("(0 * $.bananas) > 10", INDEX_FILTER_INPUT)).isNull(); // Cannot divide by zero

        parseFilterAndCompare("(9 * $.bananas) > 0", INDEX_FILTER_INPUT,
                Filter.range("bananas", 0 / 9 + 1, Long.MAX_VALUE));

        assertThatThrownBy(() -> parseFilter("($.apples * $.bananas - 5) > 10"))
                .isInstanceOf(AerospikeDSLException.class) // not supported by the current grammar
                .hasMessageContaining("Could not parse given input, wrong syntax");
    }

    @Test
    void div_twoBins() {
        assertThat(parseFilter("($.apples / $.bananas) <= 10", INDEX_FILTER_INPUT)).isNull(); // not supported by secondary index filter
    }

    @Test
    void div_binIsDivided_leftNumberIsLarger() {
        parseFilterAndCompare("($.apples / 50) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * 10 + 1, Long.MAX_VALUE)); // [501, 2^63 - 1]
        parseFilterAndCompare("($.apples / 50) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * 10, Long.MAX_VALUE)); // [500, 2^63 - 1]
        parseFilterAndCompare("($.apples / 50) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * 10 - 1)); // [-2^63, 499]
        parseFilterAndCompare("($.apples / 50) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * 10)); // [-2^63, 500]

        parseFilterAndCompare("($.apples / -50) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -50 * 10 - 1)); // [-2^63, -501]
        parseFilterAndCompare("($.apples / -50) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -50 * 10)); // [-2^63, -500]
        parseFilterAndCompare("($.apples / -50) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", -50 * 10 + 1, Long.MAX_VALUE)); // [-499, 2^63 - 1]
        parseFilterAndCompare("($.apples / -50) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", -50 * 10, Long.MAX_VALUE)); // [-500, 2^63 - 1]

        parseFilterAndCompare("($.apples / 50) > -10", INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * -10 + 1, Long.MAX_VALUE)); // [-499, 2^63 - 1]
        parseFilterAndCompare("($.apples / 50) >= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * -10, Long.MAX_VALUE)); // [-500, 2^63 - 1]
        parseFilterAndCompare("($.apples / 50) < -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * -10 - 1)); // [-2^63, -501]
        parseFilterAndCompare("($.apples / 50) <= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * -10)); // [-2^63, -500]

        parseFilterAndCompare("($.apples / -50) > -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -50 * -10 - 1)); // [-2^63, 499]
        parseFilterAndCompare("($.apples / -50) >= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 * -50)); // [-2^63, 500]
        parseFilterAndCompare("($.apples / -50) < -10", INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -50 + 1, Long.MAX_VALUE)); // [501, 2^63 - 1]
        parseFilterAndCompare("($.apples / -50) <= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -50, Long.MAX_VALUE)); // [500, 2^63 - 1]
    }

    @Test
    void div_binIsDivided_leftNumberIsSmaller() {
        parseFilterAndCompare("($.apples / 5) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 10 + 1, Long.MAX_VALUE)); // [51, 2^63 - 1]
        parseFilterAndCompare("($.apples / 5) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 10, Long.MAX_VALUE)); // [50, 2^63 - 1]
        parseFilterAndCompare("($.apples / 5) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 10 - 1)); // [-2^63, 49]
        parseFilterAndCompare("($.apples / 5) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 10)); // [-2^63, 50]

        parseFilterAndCompare("($.apples / -5) > 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * 10 - 1)); // [-2^63, -51]
        parseFilterAndCompare("($.apples / -5) >= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * 10)); // [-2^63, -50]
        parseFilterAndCompare("($.apples / -5) < 10", INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * 10 + 1, Long.MAX_VALUE)); // [-49, 2^63 - 1]
        parseFilterAndCompare("($.apples / -5) <= 10", INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * 10, Long.MAX_VALUE)); // [-50, 2^63 - 1]

        parseFilterAndCompare("($.apples / 5) > -10", INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * -10 + 1, Long.MAX_VALUE)); // [-49, 2^63 - 1]
        parseFilterAndCompare("($.apples / 5) >= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * -10, Long.MAX_VALUE)); // [-50, 2^63 - 1]
        parseFilterAndCompare("($.apples / 5) < -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * -10 - 1)); // [-2^63, -51]
        parseFilterAndCompare("($.apples / 5) <= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * -10)); // [-2^63, -50]

        parseFilterAndCompare("($.apples / -5) > -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * -10 - 1)); // [-2^63, 49]
        parseFilterAndCompare("($.apples / -5) >= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 * -5)); // [-2^63, 50]
        parseFilterAndCompare("($.apples / -5) < -10", INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -5 + 1, Long.MAX_VALUE)); // [51, 2^63 - 1]
        parseFilterAndCompare("($.apples / -5) <= -10", INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -5, Long.MAX_VALUE)); // [50, 2^63 - 1]
    }

    @Test
    void div_binIsDivided_leftNumberEqualsRight() {
        parseFilterAndCompare("($.apples / 5) > 5", INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 5 + 1, Long.MAX_VALUE)); // [26, 2^63 - 1]
        parseFilterAndCompare("($.apples / 5) >= 5", INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 5, Long.MAX_VALUE)); // [25, 2^63 - 1]
        parseFilterAndCompare("($.apples / 5) < 5", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 5 - 1)); // [-2^63, 24]
        parseFilterAndCompare("($.apples / 5) <= 5", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 5)); // [-2^63, 25]

        parseFilterAndCompare("($.apples / -5) > -5", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * -5 - 1)); // [-2^63, 24]
        parseFilterAndCompare("($.apples / -5) >= -5", INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * -5)); // [-2^63, 25]
        parseFilterAndCompare("($.apples / -5) < -5", INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * -5 + 1, Long.MAX_VALUE)); // [26, 2^63 - 1]
        parseFilterAndCompare("($.apples / -5) <= -5", INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * -5, Long.MAX_VALUE)); // [25, 2^63 - 1]
    }

    @Test
    void div_binIsDivisor_leftNumberIsLarger() {
        parseFilterAndCompare("(90 / $.bananas) > 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 1, 90 / 10 - 1)); // [1,8]
        parseFilterAndCompare("(90 / $.bananas) >= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 1, 90 / 10)); // [1,9]

        // Not supported by secondary index filter
        assertThat(parseFilter("(90 / $.bananas) < 10", INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter("(90 / $.bananas) <= 10", INDEX_FILTER_INPUT)).isNull();

        // Not supported by secondary index filter
        assertThat(parseFilter("(90 / $.bananas) > -10", INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter("(90 / $.bananas) >= -10", INDEX_FILTER_INPUT)).isNull();
        parseFilterAndCompare("(90 / $.bananas) < -10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 90 / -10 + 1, -1)); // [-8, -1]
        parseFilterAndCompare("(90 / $.bananas) <= -10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 90 / -10, -1)); // [-8, -1]

        parseFilterAndCompare("(-90 / $.bananas) > 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", -90 / 10 + 1, -1)); // [-8, -1]
        parseFilterAndCompare("(90 / $.bananas) >= 10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 1, 90 / 10)); // [1,9]
        // Not supported by secondary index filter
        assertThat(parseFilter("(-90 / $.bananas) < 10", INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter("(-90 / $.bananas) <= 10", INDEX_FILTER_INPUT)).isNull();

        // Not supported by secondary index filter
        assertThat(parseFilter("(-90 / $.bananas) > -10", INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter("(-90 / $.bananas) >= -10", INDEX_FILTER_INPUT)).isNull();
        parseFilterAndCompare("(-90 / $.bananas) < -10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 1L, -90 / -10 - 1)); // [1, 8]
        parseFilterAndCompare("(-90 / $.bananas) <= -10", INDEX_FILTER_INPUT,
                Filter.range("bananas", 1L, -90 / -10)); // [1, 9]
    }

    @Test
    void div_binIsDivisor_leftNumberIsSmaller() {
        // Not supported by secondary index filter
        assertThat(parseFilter("(9 / $.bananas) > 10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter("(9 / $.bananas) >= 10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter("(9 / $.bananas) < 10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(9 / $.bananas) <= 10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        assertThat(parseFilter("(9 / $.bananas) > -10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(9 / $.bananas) >= -10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(9 / $.bananas) < -10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter("(9 / $.bananas) <= -10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers

        assertThat(parseFilter("(-9 / $.bananas) > 10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter("(-9 / $.bananas) >= 10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter("(-9 / $.bananas) < 10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(-9 / $.bananas) <= 10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        assertThat(parseFilter("(-9 / $.bananas) > -10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(-9 / $.bananas) >= -10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(-9 / $.bananas) < -10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter("(-9 / $.bananas) <= -10", INDEX_FILTER_INPUT)).isNull(); // no integer numbers

        // Not supported by secondary index Filter
        assertThat(parseFilter("(0 / $.bananas) > 10", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        // Not supported by secondary index Filter, cannot divide by zero
        assertThat(parseFilter("(9 / $.bananas) > 0", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
    }

    @Test
    void div_binIsDivisor_leftNumberEqualsRight() {
        // Not supported by secondary index filter
        assertThat(parseFilter("(90 / $.bananas) > 90", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        parseFilterAndCompare("(90 / $.bananas) >= 90", INDEX_FILTER_INPUT,
                Filter.range("bananas", 90 / 90, 90 / 90)); // [1, 1]
        assertThat(parseFilter("(90 / $.bananas) < 90", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(90 / $.bananas) <= 90", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        assertThat(parseFilter("(-90 / $.bananas) > -90", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(-90 / $.bananas) >= -90", INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter("(-90 / $.bananas) < -90", INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        parseFilterAndCompare("(-90 / $.bananas) <= -90", INDEX_FILTER_INPUT,
                Filter.range("bananas", 1L, 90 / 90)); // [1, 1]
    }
}
