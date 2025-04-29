package com.aerospike.dsl;

import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.exception.NoApplicableFilterException;
import com.aerospike.dsl.part.AbstractPart;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DSLParserImpl implements DSLParser {

    public static final String INDEX_NAME_SEPARATOR = ".";

    @Beta
    public ParsedExpression parseExpression(String dslString) {
        ParseTree parseTree = getParseTree(dslString);
        return getParsedExpression(parseTree, null);
    }

    @Beta
    public ParsedExpression parseExpression(String input, IndexFilterInput indexFilterInput) {
        ParseTree parseTree = getParseTree(input);
        return getParsedExpression(parseTree, indexFilterInput);
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, IndexFilterInput indexFilterInput) {
        boolean hasFilterParsingError = false;
        AbstractPart resultingPart = null;
        Map<String, Index> indexesMap = new HashMap<>();
        String namespace = indexFilterInput == null ? null : indexFilterInput.getNamespace();
        Collection<Index> indexes = indexFilterInput == null ? null : indexFilterInput.getIndexes();

        if (indexes != null && !indexes.isEmpty()) {
            indexes.forEach(idx -> indexesMap.put(idx.getNamespace() + INDEX_NAME_SEPARATOR + idx.getBin(), idx));
        }
        try {
            resultingPart =
                    new ExpressionConditionVisitor().visit(parseTree);
        } catch (NoApplicableFilterException e) {
            hasFilterParsingError = true;
        }

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error
        if (!hasFilterParsingError && resultingPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }
        // Transfer the parsed tree along with namespace and indexes Map
        return new ParsedExpression(resultingPart, namespace, indexesMap);
    }
}
