package com.aerospike.dsl.parsedExpression;

import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.query.Filter;
import com.aerospike.dsl.client.query.IndexType;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseDslExpressionAndCompare;

public class LogicalParsedExpressionTests {

    @Test
    void binLogical_AND_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        TestUtils.parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp);
    }

    @Test
    void binLogical_AND_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Filter is chosen alphabetically because no cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        // Complementary Exp is provided for the remaining part of the expression
        Exp exp = Exp.gt(Exp.intBin("intBin2"), Exp.val(100));
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_one_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build());
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin2"), Exp.val(100));
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_AND_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        TestUtils.parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"),
                filter, exp);
    }

    @Test
    void binLogical_AND_OR_OR_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
        );
        TestUtils.parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100 or $.intBin4 > 100"), filter, exp);
    }

    @Test
    void binLogical_OR_AND_AND_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                )
        );
        TestUtils.parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100 and $.intBin3 > 100 and $.intBin4 > 100"), filter, exp);
    }

    @Test
    void binLogical_AND_AND_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        // Filter is chosen alphabetically because the same cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Filter is chosen alphabetically because no cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes_partial_data() {
        List<Index> indexes = List.of(
                Index.builder().namespace("other_ns").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("other_ns").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.STRING).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // intBin1/intBin2 in other namespace filtered out; intBin3 STRING wrong type; only intBin3 NUMERIC matches
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"),
                filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_AND_explicitly_given_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE); // The index has been chosen manually
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                // Manually selecting the index by its name
                IndexContext.of(TestUtils.NAMESPACE, indexes, "idx_bin1"));
    }

    @Test
    void binLogical_AND_AND_explicitly_given_index_unavailable() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Fallback to the default automatic choosing of the index
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                // There is no index with the name "intBin10"
                IndexContext.of(TestUtils.NAMESPACE, indexes, "intBin10"));
    }

    @Test
    void binLogical_AND_AND_explicitly_given_index_namespace_mismatch() {
        List<Index> indexes = List.of(
                Index.builder().namespace("other_namespace").name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        // idx_bin1 name matches but belongs to a different namespace, so falls back to automatic selection
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes, "idx_bin1"));
    }

    @Test
    void binLogical_AND_AND_explicitly_given_index_null() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin3").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Null falls back to automatic selection (highest cardinality)
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes, null));
    }

    @Test
    void binLogical_AND_AND_explicitly_given_index_empty_string() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin3").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Empty string falls back to automatic selection (highest cardinality)
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes, ""));
    }

    @Test
    void binLogical_AND_AND_explicitly_given_index_overrides_alphabetical() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin3").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        // Without hint intBin1 would be chosen alphabetically (same cardinality). With hint, intBin2 is selected.
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes, "idx_bin2"));
    }

    @Test
    void binLogical_AND_AND_with_OR_subexpression_explicitly_given_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin3").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin4").bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Without hint, intBin2 is chosen (highest cardinality). Bins in the OR are not eligible for SI filter.
        // With hint, intBin1 is selected despite lower cardinality.
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and ($.intBin3 > 100 or $.intBin4 > 100)"),
                filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes, "idx_bin1"));
    }

    @Test
    void binLogical_single_bin_explicitly_given_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100"), filter, null,
                IndexContext.of(TestUtils.NAMESPACE, indexes, "idx_bin1"));
    }

    @Test
    void binLogical_AND_AND_two_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"),
                filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.or(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        TestUtils.parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100"), filter, exp);
    }

    @Test
    void binLogical_OR_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_one_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_OR_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_OR_all_indexes_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_OR_provided_index_no_filter_created() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).name("idx_bin3").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // SI filter is never produced for a top-level OR expression, even when an explicit index is requested
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes, "idx_bin1"));
    }

    @Test
    void binLogical_prioritizedAND_OR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 > 100"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_prioritizedOR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_prioritizedOR_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        // Cardinality is the same, intBin3 is chosen because it is the only one filtered
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_prioritizedOR_indexed_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Cardinality is the same, is it correct that intBin3 is chosen because it is the only one filtered?
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_prioritizedOR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100)"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100))"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_prioritizedOR_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100)"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100))"), filter,
                exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_prioritizedAND_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100)"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100))"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR_prioritizedAND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100)"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100))"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedAND_OR_prioritizedAND_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 and $.intBin4 > 100) or" +
                        " ($.intBin2 > 100 and $.intBin1 > 100)"),
                filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 and $.intBin4 > 100) or" +
                        " ($.intBin2 > 100 and $.intBin1 > 100))"),
                filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedAND_OR_prioritizedAND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 and $.intBin4 > 100) or" +
                " ($.intBin2 > 100 and $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 and $.intBin4 > 100) or" +
                " ($.intBin2 > 100 and $.intBin1 > 100))"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedOR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 or $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 or $.intBin1 > 100))"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedOR_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 or $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 or $.intBin1 > 100))"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedAND_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 and $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 and $.intBin1 > 100))"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedAND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 and $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100) and" +
                " ($.intBin2 > 100 and $.intBin1 > 100))"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_withFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and" +
                " $.intBin1 > 100"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and" +
                " $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_withTheOnlyFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // This expression part does not have the index with the largest cardinality, but it is the only applicable
        // because all other parts participate in an OR-combined query
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and" +
                " $.intBin1 > 100"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and" +
                " $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare(ExpressionContext.of("($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and" +
                " $.intBin1 > 100"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and" +
                " $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_withFilterPerCardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // This expression part has the index with the largest cardinality and is applicable for Filter building,
        // another applicable expression part is "$.intBin1 > 100", but intBin1 has index with lower cardinality
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("(($.intBin3 > 100 or $.intBin4 > 100) and $.intBin2 > 100) and" +
                " $.intBin1 > 100"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("((($.intBin3 > 100 or $.intBin4 > 100) and $.intBin2 > 100) and" +
                " $.intBin1 > 100)"), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR2_OR1_AND2_AND_AND1_indexed_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin5").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin6").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.or(
                        Exp.or(
                                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                                Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                        ),
                        Exp.and(
                                Exp.gt(Exp.intBin("intBin5"), Exp.val(100)),
                                Exp.gt(Exp.intBin("intBin6"), Exp.val(100))
                        )
                ),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        String dslString = "(($.intBin3 > 100 or $.intBin4 > 100) or ($.intBin5 > 100 and $.intBin6 > 100)) " +
                "and ($.intBin2 > 100 and $.intBin1 > 100)";
        parseDslExpressionAndCompare(ExpressionContext.of(dslString), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(" + dslString + ")"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_OR2_OR1_AND2_AND_AND2_OR1_AND2_indexed_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin4").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin5").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin6").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin7").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin8").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // No Filter can be built as all expression parts participate in OR-combined queries
        Filter filter = null;
        Exp exp = Exp.and(
                Exp.or(
                        Exp.or(
                                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                                Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                        ),
                        Exp.and(
                                Exp.gt(Exp.intBin("intBin5"), Exp.val(100)),
                                Exp.gt(Exp.intBin("intBin6"), Exp.val(100))
                        )
                ),
                Exp.or(
                        Exp.and(
                                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                        ),
                        Exp.and(
                                Exp.gt(Exp.intBin("intBin7"), Exp.val(100)),
                                Exp.gt(Exp.intBin("intBin8"), Exp.val(100))
                        )
                )
        );
        String dslString = "(($.intBin3 > 100 or $.intBin4 > 100) or ($.intBin5 > 100 and $.intBin6 > 100)) " +
                "and (($.intBin2 > 100 and $.intBin1 > 100) or ($.intBin7 > 100 and $.intBin8 > 100))";
        parseDslExpressionAndCompare(ExpressionContext.of(dslString), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
        parseDslExpressionAndCompare(ExpressionContext.of("(" + dslString + ")"), filter, exp,
                IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void binLogical_AND_AND_bin_hint_selects_index_overrides_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Without hint intBin2 would be chosen (highest cardinality). With bin hint, intBin1 is selected.
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void binLogical_AND_AND_bin_hint_no_match_falls_back_to_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // There is no index on "intBin99", so fallback: intBin2 selected by highest cardinality
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin99"));
    }

    @Test
    void binLogical_AND_AND_bin_hint_namespace_mismatch_falls_back_to_automatic() {
        List<Index> indexes = List.of(
                Index.builder().namespace("other_namespace").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        // intBin1 bin matches but belongs to a different namespace, so falls back to automatic selection
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void binLogical_AND_AND_bin_hint_null_falls_back_to_automatic() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // Null falls back to automatic selection (highest cardinality = intBin2)
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, null));
    }

    @Test
    void binLogical_AND_AND_bin_hint_multiple_indexes_same_bin_falls_back_to_automatic() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(5).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.STRING).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        // Two indexes on intBin1 → ambiguous → fallback to full automatic; intBin1 NUMERIC wins by cardinality
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin2"), Exp.val(100));
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void binLogical_AND_AND_bin_hint_overrides_alphabetical_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        // Without hint intBin1 would be chosen alphabetically (same cardinality). With bin hint, intBin2 is selected.
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin2"));
    }

    @Test
    void binLogical_OR_bin_hint_produces_no_filter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        // SI filter is never produced for a top-level OR expression, even with a bin hint
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 or $.intBin2 > 100"), filter, exp,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void binLogical_single_bin_bin_hint_exact_match() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        parseDslExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100"), filter, null,
                IndexContext.withBinHint(TestUtils.NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void binLogical_EXCL_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.exclusive(
                Exp.eq(Exp.stringBin("hand"), Exp.val("stand")),
                Exp.eq(Exp.stringBin("pun"), Exp.val("done"))
        );
        TestUtils.parseDslExpressionAndCompare(ExpressionContext.of("exclusive($.hand == \"stand\", $.pun == \"done\")"),
                filter, exp);
    }
}
