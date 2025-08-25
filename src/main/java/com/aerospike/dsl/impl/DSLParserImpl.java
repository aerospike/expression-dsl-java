package com.aerospike.dsl.impl;

import com.aerospike.dsl.ConditionLexer;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.InputContext;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DSLParserImpl implements DSLParser {

    @Beta
    public ParsedExpression parseExpression(InputContext inputContext) {
        ParseTree parseTree = getParseTree(inputContext.getInput());
        return getParsedExpression(parseTree, inputContext.getValues(), null);
    }

    @Beta
    public ParsedExpression parseExpression(InputContext inputContext, IndexContext indexContext) {
        ParseTree parseTree = getParseTree(inputContext.getInput());
        return getParsedExpression(parseTree, inputContext.getValues(), indexContext);
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, PlaceholderValues placeholderValues,
                                                 IndexContext indexContext) {
        final String namespace = Optional.ofNullable(indexContext)
                .map(IndexContext::getNamespace)
                .orElse(null);
        final Collection<Index> indexes = Optional.ofNullable(indexContext)
                .map(IndexContext::getIndexes)
                .orElse(Collections.emptyList());

        Map<String, List<Index>> indexesMap = indexes.stream()
                // Filtering the indexes with the given namespace
                .filter(idx -> idx.getNamespace() != null && idx.getNamespace().equals(namespace))
                // Group the indexes by bin name
                .collect(Collectors.groupingBy(Index::getBin));

        AbstractPart resultingPart = new ExpressionConditionVisitor().visit(parseTree);

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error
        if (resultingPart == null) {
            throw new DslParseException("Could not parse given DSL expression input");
        }

        // Return the parsed tree along with indexes Map
        return new ParsedExpression(resultingPart, placeholderValues, indexesMap);
    }
}
