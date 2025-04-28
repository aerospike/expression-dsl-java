package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.exception.NoApplicableFilterException;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.ExpressionContainer;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.aerospike.dsl.model.AbstractPart.PartType.EXPRESSION_CONTAINER;
import static com.aerospike.dsl.visitor.VisitorUtils.*;

public class DSLParserImpl implements DSLParser {

    @Beta
    public ParsedExpression parseExpression(String input) {
        ParseTree parseTree = getParseTree(input);
        return getParsedExpression(parseTree, null, null);
    }

    @Beta
    public ParsedExpression parseExpression(String input, String namespace, Collection<Index> indexes) {
        ParseTree parseTree = getParseTree(input);
        return getParsedExpression(parseTree, namespace, indexes);
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, String namespace, Collection<Index> indexes) {
        boolean hasFilterParsingError = false;
        AbstractPart resultingPart = null;
        Map<String, Index> indexesMap = new HashMap<>();
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

    public static Pair<Filter, Exp> getResultPair(AbstractPart resultingPart, String namespace,
                                                  Map<String, Index> indexesMap) {
        if (resultingPart != null) {
            if (resultingPart.getPartType() == EXPRESSION_CONTAINER) {
                AbstractPart result =
                        buildExpr((ExpressionContainer) resultingPart, namespace, indexesMap);
                return new Pair<>(result.getFilter(), result.getExp());
            } else {
                Filter filter = resultingPart.getFilter();
                Exp exp = resultingPart.getExp();
                return new Pair<>(filter, exp);
            }
        }
        return new Pair<>(null, null);
    }
}
