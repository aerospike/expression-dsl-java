package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.exception.NoApplicableFilterException;
import com.aerospike.dsl.index.Index;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collection;

public class DSLParserImpl implements DSLParser {

    @Beta
    public ParsedExpression parseDslExpression(String input) {
        ParseTree parseTree = getParseTree(input);
        Filter sIndexFilter = parseFilter(parseTree, "test", null);
        Expression expResult = Exp.build(parseFilterExp(parseTree));
        return new ParsedExpression(expResult, sIndexFilter);
    }

    @Beta
    public ParsedExpression parseDslExpression(String input, String namespace, Collection<Index> indexes) {
        ParseTree parseTree = getParseTree(input);
        Expression expResult = Exp.build(parseFilterExp(parseTree));
        Filter sIndexFilter = parseFilter(parseTree, namespace, indexes);
        return new ParsedExpression(expResult, sIndexFilter);
    }

    @Beta
    public Expression parseFilterExpression(String input) {
        Exp result = parseFilterExp(getParseTree(input));
        if (result == null) return null;
        return Exp.build(result);
    }

    @Beta
    public Filter parseFilter(String input, String namespace, Collection<Index> indexes) {
        return parseFilter(getParseTree(input), namespace, indexes);
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private Exp parseFilterExp(ParseTree parseTree) {
        AbstractPart filterExprAbstractPart = new ExpressionConditionVisitor(true, false).visit(parseTree);

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error instead of NPE
        if (filterExprAbstractPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }

        return filterExprAbstractPart.getExp();
    }

    private Filter parseFilter(ParseTree parseTree, String namespace, Collection<Index> indexes) {
        boolean isEmptyList = false;
        AbstractPart sIndexFiltersAbstractPart = null;
        try {
            sIndexFiltersAbstractPart = new ExpressionConditionVisitor(namespace, indexes, false, true).visit(parseTree);
        } catch (NoApplicableFilterException e) {
            isEmptyList = true;
        }

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error instead of NPE
        if (!isEmptyList && sIndexFiltersAbstractPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }

        Filter sIndexFilter = null;
        if (sIndexFiltersAbstractPart != null && sIndexFiltersAbstractPart.getSIndexFilter() != null) {
            sIndexFilter = sIndexFiltersAbstractPart.getSIndexFilter();
        }
        return sIndexFilter;
    }
}
