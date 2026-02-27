package com.aerospike.dsl.parsedExpression;

import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import com.aerospike.dsl.client.query.Filter;
import com.aerospike.dsl.client.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.NAMESPACE;
import static com.aerospike.dsl.util.TestUtils.parseDslExpressionAndCompare;

class InFilterTests {

    // --- Single IN + comparison with indexes — IN always excluded from Filter ---

    @Test
    void inAndEq_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void eqAndIn_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.equal("intBin1", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin2"), Exp.val(List.of(1, 2, 3)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 == 100 and $.intBin2 in [1, 2, 3]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void eqAndInAndLt_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin3", Long.MIN_VALUE, 49);
        Exp exp = Exp.and(
                Exp.eq(Exp.intBin("intBin1"), Exp.val(100)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(1, 2, 3))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 == 100 and $.intBin2 in [1, 2, 3] and $.intBin3 < 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inAndGt_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(10, 20)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [10, 20] and $.intBin2 > 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void gtAndIn_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin2"), Exp.val(List.of(10, 20)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 > 100 and $.intBin2 in [10, 20]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- Two IN parts with indexes — never produce Filter ---

    @Test
    void twoIns_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoIns_noIndexes() {
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4]"),
                filter, exp);
    }

    @Test
    void twoInsAndEq_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin3", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4] and $.intBin3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void eqAndTwoIns_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.equal("intBin1", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin3"), Exp.val(List.of(3, 4))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 == 100 and $.intBin2 in [1, 2] and $.intBin3 in [3, 4]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInsAndLtAndGt_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("b3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("b4").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.range("b2", Long.MIN_VALUE, 49);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b3"), Exp.val(List.of(2))),
                Exp.gt(Exp.intBin("b4"), Exp.val(100)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 < 50 and $.b3 in [2] and $.b4 > 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- IN bin has highest cardinality — fallback to next best ---

    @Test
    void inBinHighestCard_fallback() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inBinHighestCard_fallbackGt() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 > 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inBinHighestCard_3exprs() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(5).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(50)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 < 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inBinOnlyIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- Two IN bins with highest cardinality — fallback ---

    @Test
    void twoInBinsHighestCard() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("b3", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b2"), Exp.val(List.of(2))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 in [2] and $.b3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInBinsHighestCard_noOther() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b2"), Exp.val(List.of(2))));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 in [2]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInBinsHighCard_withGtLt() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(5).build(),
                Index.builder().namespace(NAMESPACE).bin("b3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b4").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("b2", 51, Long.MAX_VALUE);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b3"), Exp.val(List.of(2))),
                Exp.lt(Exp.intBin("b4"), Exp.val(100)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 > 50 and $.b3 in [2] and $.b4 < 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInsOnlyIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b2"), Exp.val(List.of(2))),
                Exp.eq(Exp.intBin("b3"), Exp.val(100)));
        parseDslExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 in [2] and $.b3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }
}
