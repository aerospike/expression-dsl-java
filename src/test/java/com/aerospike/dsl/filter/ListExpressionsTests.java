package com.aerospike.dsl.filter;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.LIST;

class ListExpressionsTests {

    String NAMESPACE = "test1";
    List<Index> INDEXES = List.of(
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.NUMERIC).build(),
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.STRING)
                    .indexCollectionType(LIST).build(),
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.STRING)
                    .indexCollectionType(LIST).ctx(new CTX[]{CTX.listIndex(5)}).build(),
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.STRING)
                    .indexCollectionType(LIST).ctx(new CTX[]{CTX.listValue(Value.get(5))}).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void listExpression() {
        Filter expected = Filter.equal("listBin1", "stringVal");
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1 == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.get(type: STRING) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.listBin1.get(type: STRING, return: VALUE) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected
        );
    }

    @Test
    void listExpressionNested_oneLevel() {
        Filter expected = Filter.equal("listBin1", "stringVal", CTX.listIndex(5));
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5] == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5].get(type: STRING) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.listBin1.[5].get(type: STRING, return: VALUE) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected
        );
    }

    @Test
    void listExpressionNested_twoLevels() {
        Filter expected = Filter.equal("listBin1", "stringVal", CTX.listIndex(5), CTX.listIndex(1));
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5].[1] == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5].[1].get(type: STRING) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.listBin1.[5].[1].get(type: STRING, return: VALUE) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected
        );

        Filter expected2 = Filter.equal("listBin1", "stringVal", CTX.listValue(Value.get(5)), CTX.listRank(10));
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[=5].[#10] == \"stringVal\""),
                INDEX_FILTER_INPUT, expected2);
    }
}
