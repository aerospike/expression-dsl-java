package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.exception.NoApplicableFilterException;
import com.aerospike.dsl.index.Index;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.Expr;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collection;

import static com.aerospike.dsl.model.AbstractPart.PartType.EXPR;
import static com.aerospike.dsl.visitor.VisitorUtils.buildExpr;

public class DSLParserImpl implements DSLParser {

    @Beta
    public ParsedExpression parseDslExpression(String input) {
        ParseTree parseTree = getParseTree(input);
        return getParsedExpression(parseTree, null, null);
    }

    @Beta
    public ParsedExpression parseDslExpression(String input, String namespace, Collection<Index> indexes) {
        ParseTree parseTree = getParseTree(input);
        return getParsedExpression(parseTree, namespace, indexes);
    }

    @Beta
    public Expression parseFilterExpression(String input) {
        return getFilterExpression(getParseTree(input), null, null);
    }

    @Beta
    public Exp parseFilterExp(String input) {
        return getFilterExp(getParseTree(input), null, null);
    }

    @Beta
    public Filter parseFilter(String input, String namespace, Collection<Index> indexes) {
        return getSIFilter(getParseTree(input), namespace, indexes);
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, String namespace, Collection<Index> indexes) {
        return getParsedExpression(parseTree, namespace, indexes, false, false);
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, String namespace, Collection<Index> indexes,
                                                 boolean isFilterExpOnly, boolean isSIFilterOnly) {
        boolean isEmptyList = false;
        AbstractPart resultingPart = null;
        try {
            resultingPart =
                    new ExpressionConditionVisitor(namespace, indexes, isFilterExpOnly, isSIFilterOnly).visit(parseTree);
        } catch (NoApplicableFilterException e) {
            isEmptyList = true;
        }

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error instead of NPE
        if (!isEmptyList && resultingPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }

        if (resultingPart != null) {
            if (resultingPart.getPartType() == EXPR) {
                AbstractPart result =
                        buildExpr((Expr) resultingPart, namespace, indexes, isFilterExpOnly, isSIFilterOnly);
                return new ParsedExpression(result.getExp(), result.getSIndexFilter());
            } else {
                Filter filter = null;
                if (!isFilterExpOnly) filter = resultingPart.getSIndexFilter();
                Exp exp = null;
                if (!isSIFilterOnly) exp = resultingPart.getExp();
                return new ParsedExpression(exp, filter);
            }
        }
        return new ParsedExpression(null, null);
    }

    private Exp getFilterExp(ParseTree parseTree, String namespace, Collection<Index> indexes) {
        return getParsedExpression(parseTree, namespace, indexes, true, false).getFilterExp();
    }

    private Expression getFilterExpression(ParseTree parseTree, String namespace, Collection<Index> indexes) {
        return getParsedExpression(parseTree, namespace, indexes, true, false).getFilterExpression();
    }

    private Filter getSIFilter(ParseTree parseTree, String namespace, Collection<Index> indexes) {
        return getParsedExpression(parseTree, namespace, indexes, false, true).getSIFilter();
    }
}
