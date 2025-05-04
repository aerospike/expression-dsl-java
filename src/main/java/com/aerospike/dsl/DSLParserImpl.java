package com.aerospike.dsl;

import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exceptions.ParseException;
import com.aerospike.dsl.exceptions.NoApplicableFilterException;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String namespace;
        Collection<Index> indexes;
        if (indexContext != null) {
            namespace = indexContext.getNamespace();
            indexes = indexContext.getIndexes();
        } else {
            namespace = null;
            indexes = null;
        }

        boolean hasFilterParsingError = false;
        AbstractPart resultingPart = null;
        Map<String, List<Index>> indexesMap = new HashMap<>();

        if (indexes != null && !indexes.isEmpty()) {
            indexes.forEach(idx -> {
                    // Filtering the indexes with the given namespace
                    if (idx.getNamespace() != null && idx.getNamespace().equals(namespace)) {
                        // Group the indexes by bin name
                        indexesMap.computeIfAbsent(idx.getBin(), k -> new ArrayList<>()).add(idx);
                    }
                }
            );
        }
        try {
            resultingPart = new ExpressionConditionVisitor().visit(parseTree);
        } catch (NoApplicableFilterException e) {
            hasFilterParsingError = true;
        }

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error
        if (!hasFilterParsingError && resultingPart == null) {
            throw new ParseException("Could not parse given input, wrong syntax");
        }
        // Transfer the parsed tree along with namespace and indexes Map
        return new ParsedExpression(resultingPart, indexesMap);
    }
}
