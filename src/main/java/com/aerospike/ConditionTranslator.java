package com.aerospike;

import com.aerospike.client.exp.Expression;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;


public class ConditionTranslator {

    public static Expression translate(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.parse();

        ExpressionConditionVisitor visitor = new ExpressionConditionVisitor();
        return visitor.visit(tree);
    }
}
