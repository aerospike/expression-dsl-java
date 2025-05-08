package com.aerospike.dsl.parsedExpression;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
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
        TestUtils.parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", filter, exp);
    }

    @Test
    void binLogical_AND_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).build()
        );
        String namespace = "test1";
        // Filter is chosen alphabetically because no cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin2"), Exp.val(100));
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_one_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build());
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin2"), Exp.val(100));
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        String namespace = "test1";
        // Filter is chosen alphabetically because the same cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).build()
        );
        String namespace = "test1";
        // Filter is chosen alphabetically because no cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_AND_all_indexes_partial_data() {
        List<Index> indexes = List.of(
                Index.builder().bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.STRING).binValuesRatio(0).build(),
                // The only matching index with full data
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        // The only matching index with full data is for intBin3
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_AND_two_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.or(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        TestUtils.parseDslExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100", filter, exp);
    }

    @Test
    void binLogical_OR_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_one_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_OR_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_OR_all_indexes_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedAND_OR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 > 100", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_prioritizedOR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_prioritizedOR_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        // Cardinality is the same, is it correct that intBin3 is chosen because it is the only one filtered?
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_AND_prioritizedOR_indexed_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).build()
        );
        String namespace = "test1";
        // Cardinality is the same, is it correct that intBin3 is chosen because it is the only one filtered?
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_prioritizedOR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_prioritizedOR_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_prioritizedAND_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_OR_prioritizedAND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = null;
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare("$.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100)", filter, exp,
                IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("($.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100))", filter, exp,
                IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedAND_OR_prioritizedAND_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
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
        parseDslExpressionAndCompare("($.intBin3 > 100 and $.intBin4 > 100) or ($.intBin2 > 100 and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 and $.intBin4 > 100) or ($.intBin2 > 100 and $.intBin1 > 100))",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedAND_OR_prioritizedAND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
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
        parseDslExpressionAndCompare("($.intBin3 > 100 and $.intBin4 > 100) or ($.intBin2 > 100 and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 and $.intBin4 > 100) or ($.intBin2 > 100 and $.intBin1 > 100))",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedOR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
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
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 or $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 or $.intBin1 > 100))",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedOR_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
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
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 or $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 or $.intBin1 > 100))",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedAND_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100))
        );
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 and $.intBin1 > 100))",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_AND_prioritizedAND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.or(
                        Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100))
                ),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
        );
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 and $.intBin1 > 100))",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_withFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_withTheOnlyFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
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
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.or(
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100)),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin4"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                )
        );
        parseDslExpressionAndCompare("($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
    }

    @Test
    void binLogical_prioritizedOR_prioritizedAND_AND_indexed_withFilterPerCardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
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
        parseDslExpressionAndCompare("(($.intBin3 > 100 or $.intBin4 > 100) and $.intBin2 > 100) and $.intBin1 > 100",
                filter, exp, IndexContext.of(namespace, indexes));
        parseDslExpressionAndCompare("((($.intBin3 > 100 or $.intBin4 > 100) and $.intBin2 > 100) and $.intBin1 > 100)",
                filter, exp, IndexContext.of(namespace, indexes));
    }
}
