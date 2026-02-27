package com.aerospike.dsl.impl;

import com.aerospike.dsl.ConditionLexer;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.client.cdt.CTX;
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

import static com.aerospike.dsl.visitor.VisitorUtils.buildCtx;

public class DSLParserImpl implements DSLParser {

    @Override
    @Beta
    public ParsedExpression parseExpression(ExpressionContext expressionContext) {
        ParseTree parseTree = getParseTree(expressionContext.getExpression());
        return getParsedExpression(parseTree, expressionContext.getValues(), null);
    }

    @Override
    @Beta
    public ParsedExpression parseExpression(ExpressionContext expressionContext, IndexContext indexContext) {
        ParseTree parseTree = getParseTree(expressionContext.getExpression());
        return getParsedExpression(parseTree, expressionContext.getValues(), indexContext);
    }

    @Override
    @Beta
    public CTX[] parseCTX(String pathToCtx) {
        if (pathToCtx == null || pathToCtx.isBlank()) {
            throw new DslParseException("Path must not be null or empty");
        }

        ParseTree parseTree = getParseTree(pathToCtx);
        try {
            return buildCtx(new ExpressionConditionVisitor().visit(parseTree));
        } catch (Exception e) {
            throw new DslParseException("Could not parse the given DSL path input", e);
        }
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        ConditionParser parser = new ConditionParser(tokenStream);
        parser.addErrorListener(new DSLParserErrorListener());
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, PlaceholderValues placeholderValues,
                                                 IndexContext indexContext) {
        final String namespace = Optional.ofNullable(indexContext)
                .map(IndexContext::getNamespace)
                .orElse(null);

        Map<String, List<Index>> indexesMap = buildIndexesMap(
                Optional.ofNullable(indexContext).map(IndexContext::getIndexes).orElse(null), namespace);
        String preferredBin = Optional.ofNullable(indexContext)
                .map(IndexContext::getPreferredBin)
                .orElse(null);

        AbstractPart resultingPart = new ExpressionConditionVisitor().visit(parseTree);

        if (resultingPart == null) {
            throw new DslParseException("Could not parse given DSL expression input");
        }

        return new ParsedExpression(resultingPart, placeholderValues, indexesMap, preferredBin);
    }

    private Map<String, List<Index>> buildIndexesMap(Collection<Index> indexes, String namespace) {
        if (indexes == null || indexes.isEmpty() || namespace == null) return Collections.emptyMap();
        return indexes.stream()
                .filter(idx -> namespace.equals(idx.getNamespace()))
                .collect(Collectors.groupingBy(Index::getBin));
    }
}
