package com.aerospike.dsl;

import com.aerospike.dsl.annotation.Beta;
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
    public ParsedExpression parseExpression(String dslString) {
        ParseTree parseTree = getParseTree(dslString);
        return getParsedExpression(parseTree, null);
    }

    @Beta
    public ParsedExpression parseExpression(String input, IndexContext indexContext) {
        ParseTree parseTree = getParseTree(input);
        return getParsedExpression(parseTree, indexContext);
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, IndexContext indexContext) {
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
        return new ParsedExpression(resultingPart, indexesMap);
    }
}
