package com.aerospike.dsl.parsedExpression;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.index.Index;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseExpressionAndCompare;

public class LogicalParsedExpressionTests {

    @Test
    void binLogical_AND_2_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", expected);
    }

    @Test
    void binLogical_AND_2_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_2_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).build()
        );
        String namespace = "test1";
        // Filter is chosen alphabetically because no cardinality is given
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_2_one_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build());
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_3_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_3_all_indexes_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_3_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_3_all_indexes_partial_data() {
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
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_3_two_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(100))
        );
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_OR_2_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.or(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100", expected);
    }

    @Test
    void binLogical_OR_2_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = null;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_OR_2_one_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = null;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_OR_3_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = null;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_OR_3_all_indexes_no_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = null;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_OR_3_all_indexes_same_cardinality() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = null;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", namespace, indexes, expected);
    }

    @Test
    void binLogical_AND_OR_indexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = null;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100", namespace, indexes, expected);
        parseExpressionAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 > 100", namespace, indexes, expected);
    }

    @Disabled // TODO: complex logical structures
    @Test
    void binLogical_AND_indexed_OR() {
        List<Index> indexes = List.of(
                Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace("test1").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("intBin3").indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        String namespace = "test1";
        Filter filter = Filter.range("intBin3", 101, Long.MAX_VALUE);
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));;
        ParsedExpression expected = new ParsedExpression(exp, filter);
        parseExpressionAndCompare("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100", namespace, indexes, expected);
        parseExpressionAndCompare("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 > 100", namespace, indexes, expected);
    }
}
