package com.aerospike.dsl.parsedExpression;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.InputContext;
import com.aerospike.dsl.ParseResult;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PlaceholdersTests {

    @Test
    void intBin_GT_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)),
                filter, exp);
    }

    @Test
    void intBin_GT_no_indexes_reuseExprTree() {
        ParsedExpression parsedExpr =
                TestUtils.getParsedExpression(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)), null);
        ParseResult result =
                TestUtils.getParseResult(parsedExpr.getExprTree(), parsedExpr.getPlaceholderValues(), parsedExpr.getIndexesMap());

        assertThat(result.getFilter()).isNull();
        Expression expToCompare = Exp.build(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        assertThat(Exp.build(result.getExp())).isEqualTo(expToCompare);
    }

    @Test
    void intBin_GT_has_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = null;
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)),
                filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }

    @Test
    void intBin_GT_has_index_reuseExprTree() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        ParsedExpression parsedExpr =
                TestUtils.getParsedExpression(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)),
                        IndexContext.of(TestUtils.NAMESPACE, indexes));
        ParseResult result =
                TestUtils.getParseResult(parsedExpr.getExprTree(), parsedExpr.getPlaceholderValues(), parsedExpr.getIndexesMap());

        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        assertThat(result.getFilter()).isEqualTo(filter);
        assertThat(result.getExp()).isNull();
    }

    @Test
    void intBin_GT_AND_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)), Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0 and $.intBin2 > ?1",
                new PlaceholderValues(100, 100)), filter, exp);
    }

    @Test
    void intBin_GT_AND_all_indexes() {
        List<Index> indexes = List.of(
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(TestUtils.NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0 and $.intBin2 > ?1",
                new PlaceholderValues(100, 100)), filter, exp, IndexContext.of(TestUtils.NAMESPACE, indexes));
    }
}
