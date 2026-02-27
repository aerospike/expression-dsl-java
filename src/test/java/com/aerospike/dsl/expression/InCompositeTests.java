package com.aerospike.dsl.expression;

import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.dsl.util.TestUtils.parseFilterExpressionAndCompare;

class InCompositeTests {

    @Test
    void inWithAndOperator() {
        Exp expected = Exp.and(
                Exp.gt(Exp.intBin("cost"), Exp.val(50)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("status"), Exp.val(List.of("active", "pending"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.cost > 50 and $.status in [\"active\", \"pending\"]"), expected);
    }

    @Test
    void inWithOrOperator() {
        Exp expected = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("status"), Exp.val(List.of("active"))),
                Exp.gt(Exp.intBin("priority"), Exp.val(5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.status in [\"active\"] or $.priority > 5"), expected);
    }

    @Test
    void complexExpressionWithIn() {
        Exp expected = Exp.and(
                Exp.gt(Exp.intBin("cost"), Exp.val(50)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("status"), Exp.listBin("allowedStatuses")),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.val("available"), Exp.listBin("bookableStates")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.cost > 50 and $.status in $.allowedStatuses" +
                        " and \"available\" in $.bookableStates"), expected);
    }

    @Test
    void inWithParentheses() {
        Exp expected = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.gt(Exp.intBin("age"), Exp.val(18)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("($.name in [\"Bob\"]) and $.age > 18"), expected);
    }

    @Test
    void inInsideWithStructure() {
        Exp expected = Exp.let(
                Exp.def("allowed", Exp.val(List.of("Bob", "Mary"))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.var("allowed")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("with(allowed = [\"Bob\", \"Mary\"])" +
                        " do ($.name.get(type: STRING) in ${allowed})"), expected);
    }

    @Test
    void inInsideWhenCondition() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.val("VIP"),
                Exp.val("regular"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.name.get(type: STRING) in [\"Bob\"] => \"VIP\"," +
                        " default => \"regular\")"), expected);
    }

    @Test
    void notWrappingIn() {
        Exp expected = Exp.not(ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("not($.name in [\"Bob\", \"Mary\"])"), expected);
    }

    @Test
    void arithmeticExprAsLeftIn() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.add(Exp.intBin("a"), Exp.val(5)),
                Exp.val(List.of(10, 20, 30)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a + 5 in [10, 20, 30]"), expected);
    }
}
