package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.part.AbstractPart;
import com.aerospike.dsl.part.ExpressionContainer;
import lombok.Getter;

import java.util.Map;

import static com.aerospike.dsl.part.AbstractPart.PartType.EXPRESSION_CONTAINER;
import static com.aerospike.dsl.visitor.VisitorUtils.buildExpr;

/**
 * A class to build and store the results of DSL expression parsing: secondary index {@link Filter}
 * and/or filter {@link Exp} and {@link Expression}.
 */
@Beta
@Getter
public class ParsedExpression {

    private final AbstractPart expressionTree;
    private final String namespace;
    private final Map<String, Index> indexesMap;
    private Result result;

    public ParsedExpression(AbstractPart expressionTree, String namespace, Map<String, Index> indexesMap) {
        this.expressionTree = expressionTree;
        this.namespace = namespace;
        this.indexesMap = indexesMap;
    }

    /**
     * @return Pair of secondary index {@link Filter} and filter {@link Exp}. Each can be null in case of invalid or
     * unsupported DSL string
     * @throws AerospikeDSLException If there was an error
     */
    public Result getResult() {
        if (result == null) {
            result = getResultPair();
        }
        return result;
    }

    private Result getResultPair() {
        if (expressionTree != null) {
            if (expressionTree.getPartType() == EXPRESSION_CONTAINER) {
                AbstractPart result = buildExpr((ExpressionContainer) expressionTree, namespace, indexesMap);
                return new Result(result.getFilter(), result.getExp());
            } else {
                Filter filter = expressionTree.getFilter();
                Exp exp = expressionTree.getExp();
                return new Result(filter, exp);
            }
        }
        return new Result(null, null);
    }
}
