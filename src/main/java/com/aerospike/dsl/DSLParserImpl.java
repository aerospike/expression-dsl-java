package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.AbstractPart;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;

public class DSLParserImpl implements DSLParser {

    @Beta
    public Expression parseExpression(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.parse();

        ExpressionConditionVisitor visitor = new ExpressionConditionVisitor();
        AbstractPart abstractPart = visitor.visit(tree);

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error instead of NPE
        if (abstractPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }
        Exp expResult = abstractPart.getExp();
        return Exp.build(expResult);
    }

    @Beta
    public List<Filter> parseFilters(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.parse();

        FilterConditionVisitor visitor = new FilterConditionVisitor();
        AbstractPart abstractPart = visitor.visit(tree);

        if (abstractPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }
        return abstractPart.getFilters().getFilters();
    }
}
