package com.aerospike.dsl.parsedExpression;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
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

import java.util.Base64;
import java.util.Collection;
import java.util.List;

import static com.aerospike.dsl.util.TestUtils.NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PlaceholdersTests {

    @Test
    void intBin_GT_no_indexes() {
        Filter filter = null;
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)),
                filter, exp);

        Exp expString = Exp.gt(Exp.stringBin("strBin1"), Exp.val("str"));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.strBin1 > ?0", new PlaceholderValues("str")),
                filter, expString);
        Exp expString2 = Exp.gt(Exp.stringBin("strBin1"), Exp.val("'str'"));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.strBin1 > ?0", new PlaceholderValues("'str'")),
                filter, expString2);
        Exp expString3 = Exp.gt(Exp.stringBin("strBin1"), Exp.val("\"str\""));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.strBin1 > ?0", new PlaceholderValues("\"str\"")),
                filter, expString3);

        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        Exp expStringBase64 = Exp.gt(Exp.blobBin("blobBin1"), Exp.val(data));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.blobBin1.get(type: BLOB) > ?0",
                new PlaceholderValues(encodedString)), filter, expStringBase64);
    }

    @Test
    void intBin_GT_no_indexes_reuseExprTree() {
        ParsedExpression parsedExpr =
                TestUtils.getParsedExpression(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)), null);
        ParseResult result = parsedExpr.getResult(new PlaceholderValues(200));

        assertThat(result.getFilter()).isNull();
        Expression expToCompare = Exp.build(Exp.gt(Exp.intBin("intBin1"), Exp.val(200)));
        assertThat(Exp.build(result.getExp())).isEqualTo(expToCompare);
    }

    @Test
    void intBin_GT_no_indexes_size_mismatch() {
        assertThatThrownBy(() ->
                TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0", new PlaceholderValues()),
                        null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for placeholder ?0");

        assertThatThrownBy(() ->
                TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0 and $.intBin2 > ?1",
                        new PlaceholderValues(100)), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for placeholder ?1");

        Filter filter = null;
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        // If there are more values than placeholders we only match the required indexes
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100, 200)),
                filter, exp);
    }

    @Test
    void intBin_GT_has_index() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = null;
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void intBin_GT_has_index_reuseExprTree() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        ParsedExpression parsedExpr =
                TestUtils.getParsedExpression(InputContext.of("$.intBin1 > ?0", new PlaceholderValues(100)),
                        IndexContext.of(NAMESPACE, indexes));
        ParseResult result = parsedExpr.getResult(new PlaceholderValues(200));

        Filter filter = Filter.range("intBin1", 201, Long.MAX_VALUE);
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
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = Exp.gt(Exp.intBin("intBin1"), Exp.val(100));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.intBin1 > ?0 and $.intBin2 > ?1",
                new PlaceholderValues(100, 100)), filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void metadataExpression_TTL() {
        // Expression to find records that will expire within 24 hours
        Filter filter = null;

        Exp exp = Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("$.ttl() <= ?0", new PlaceholderValues(86400)),
                filter, exp);
    }

    @Test
    void arithmeticExpression() {
        Filter filter = null;
        Exp exp = Exp.gt(Exp.add(Exp.intBin("apples"), Exp.val(5)), Exp.val(10));
        TestUtils.parseDslExpressionAndCompare(InputContext.of("($.apples + ?0) > ?1", new PlaceholderValues(5, 10)),
                filter, exp);

        Collection<Index> INDEXES = List.of(
                Index.builder().namespace("test1").bin("apples").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace("test1").bin("bananas").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);
        Filter filter2 = Filter.range("apples", 10 - 5 + 1, Long.MAX_VALUE);
        Exp exp2 = null;
        TestUtils.parseDslExpressionAndCompare(InputContext.of("($.apples + ?0) > ?1", new PlaceholderValues(5, 10)),
                filter2, exp2, INDEX_FILTER_INPUT);
    }

    @Test
    void mapNestedExp() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.mapBin1.a.bb.bcc > ?0", new PlaceholderValues(200)),
                expected);
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.mapBin1.a.bb.bcc.get(type: INT) > ?0",
                new PlaceholderValues(200)), expected);
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > ?0",
                new PlaceholderValues(200)), expected);

        // String
        expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.mapBin1.a.bb.bcc == ?0",
                new PlaceholderValues("stringVal")), expected);
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.mapBin1.a.bb.bcc.get(type: STRING) == ?0",
                new PlaceholderValues("stringVal")), expected);
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE) == ?0",
                new PlaceholderValues("stringVal")), expected);
    }

    @Test
    void nestedListsExpWithDifferentContextTypes() {
        // Nested List Rank
        Exp expected = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(-1),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5)
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.listBin1.[5].[#-1] == ?0",
                new PlaceholderValues("stringVal")), expected);
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.listBin1.[5].[#-1].get(type: STRING) == ?0",
                new PlaceholderValues("stringVal")), expected);

        // Nested List Rank Value
        expected = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(100),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5),
                        CTX.listRank(-1)
                ),
                Exp.val(200));
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.listBin1.[5].[#-1].[=100] == ?0",
                new PlaceholderValues(200)), expected);
    }

    @Test
    void forthDegreeComplicatedExplicitFloat() {
        TestUtils.parseFilterExpressionAndCompare(InputContext.of(
                        "(($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT))" +
                                " + ($.oranges.get(type: FLOAT) + $.acai.get(type: FLOAT))) > ?0", new PlaceholderValues(10.5)),
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")),
                                Exp.add(Exp.floatBin("oranges"), Exp.floatBin("acai"))),
                        Exp.val(10.5))
        );
    }

    @Test
    void complicatedWhenExplicitTypeString() {
        Exp expected = Exp.eq(
                Exp.stringBin("a"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(1)
                        ), Exp.stringBin("a1"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(2)
                        ), Exp.stringBin("a2"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(3)
                        ), Exp.stringBin("a3"),
                        Exp.val("hello")
                )
        );

        TestUtils.parseFilterExpressionAndCompare(InputContext.of("$.a.get(type: STRING) == " +
                "(when($.b == ?0 => $.a1.get(type: STRING)," +
                " $.b == ?1 => $.a2.get(type: STRING)," +
                " $.b == ?2 => $.a3.get(type: STRING)," +
                " default => ?3))", new PlaceholderValues(1, 2, 3, "hello")), expected);
    }

    @Test
    void whenWithMultipleDeclarations() {
        Exp expected = Exp.cond(
                Exp.eq(
                        Exp.intBin("who"),
                        Exp.val(1)
                ), Exp.val("bob"),
                Exp.eq(
                        Exp.intBin("who"),
                        Exp.val(2)
                ), Exp.val("fred"),
                Exp.val("other")
        );

        TestUtils.parseFilterExpressionAndCompare(InputContext.of("when ($.who == ?0 => ?1, " +
                        "$.who == ?2 => ?3, default => ?4)", new PlaceholderValues(1, "bob", 2, "fred", "other")),
                expected);
    }
}
