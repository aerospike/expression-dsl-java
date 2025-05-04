package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exceptions.DslParseException;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;
import java.util.Map;

import static com.aerospike.dsl.parts.AbstractPart.PartType.EXPRESSION_CONTAINER;
import static com.aerospike.dsl.visitor.VisitorUtils.buildExpr;


/**
 * A class to build and store the results of DSL expression parsing: {@link ParseResult} that holds
 * a potential secondary index {@link Filter} and a potential filter {@link Exp}, and parsed {@code expressionTree}.
 */
@Beta
@Getter
public class ParsedExpression {

    private final AbstractPart expressionTree;
    private final Map<String, List<Index>> indexesMap;
    private ParseResult result;

    public ParsedExpression(AbstractPart expressionTree, Map<String, List<Index>> indexesMap) {
        this.expressionTree = expressionTree;
        this.indexesMap = indexesMap;
    }

    /**
     * @return Pair of secondary index {@link Filter} and filter {@link Exp}. Each can be null in case of invalid or
     * unsupported DSL string
     * @throws DslParseException If there was an error
     */
    public ParseResult getResult() {
        if (result == null) {
            result = getParseResult();
        }
        return result;
    }

    private ParseResult getParseResult() {
        if (expressionTree != null) {
            if (expressionTree.getPartType() == EXPRESSION_CONTAINER) {
                AbstractPart result = buildExpr((ExpressionContainer) expressionTree, indexesMap);
                return new ParseResult(result.getFilter(), result.getExp());
            } else {
                Filter filter = expressionTree.getFilter();
                Exp exp = expressionTree.getExp();
                return new ParseResult(filter, exp);
            }
        }
        return new ParseResult(null, null);
    }
}
